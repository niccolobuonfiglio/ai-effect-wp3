"""Redis-based task queue for workflow task distribution.
"""

import time
from collections.abc import Callable

from redis import Redis
from redis.client import Pipeline

DEFAULT_LEASE_SECONDS = 300.0

Writer = Callable[[Pipeline], None]
Hook = Callable[[Pipeline, str], Writer | None]


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class RedisTaskQueue:
    """Task queue for distributing tasks to workers."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    def _client(self, client):
        return client if client is not None else self._redis

    def queue_key(self, workflow_id: str) -> str:
        return f"queue:{workflow_id}"

    def delayed_key(self, workflow_id: str) -> str:
        return f"delayed:{workflow_id}"

    def attempts_key(self, workflow_id: str) -> str:
        return f"attempts:{workflow_id}"

    def blocked_since_key(self, workflow_id: str) -> str:
        return f"blocked_since:{workflow_id}"

    def inputs_key(self, workflow_id: str, task_id: str) -> str:
        return f"inputs:{workflow_id}:{task_id}"

    def queued_key(self, workflow_id: str, task_id: str) -> str:
        return f"queued:{workflow_id}:{task_id}"

    def processing_key(self, workflow_id: str) -> str:
        return f"processing:{workflow_id}"

    # inputs handed from one task to the next
    def add_inputs(
        self, workflow_id: str, task_id: str, source: str, payload: str, client=None
    ) -> None:
        """Store the inputs source produced for task_id.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not source:
            raise ValueError("source is required")
        self._client(client).hset(self.inputs_key(workflow_id, task_id), source, payload)

    def inputs(self, workflow_id: str, task_id: str, client=None) -> list[str]:
        """Stored inputs for a task, ordered by producer name.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        stored = self._client(client).hgetall(self.inputs_key(workflow_id, task_id)) or {}
        decoded = {_decode(k): _decode(v) for k, v in stored.items()}
        return [decoded[name] for name in sorted(decoded)]

    # queueing a task at most once
    def claim_enqueue(self, workflow_id: str, task_id: str) -> bool:
        """Enqueue a task unless another worker already did; True if enqueued.

        Two predecessors of the same task can both see "no dependencies left";
        only the one that wins this guard queues it.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        def claim(pipe):
            if self.is_enqueued(workflow_id, task_id, client=pipe):
                return False
            pipe.multi()
            self.enqueue_once(workflow_id, task_id, client=pipe)
            return True

        return self._redis.transaction(
            claim, self.queued_key(workflow_id, task_id), value_from_callable=True
        )

    def is_enqueued(self, workflow_id: str, task_id: str, client=None) -> bool:
        """Whether the at-most-once guard of a task has already fired."""
        return bool(self._client(client).exists(self.queued_key(workflow_id, task_id)))

    def enqueue_once(self, workflow_id: str, task_id: str, client=None) -> None:
        """Set a task's guard and enqueue it.
        """
        r = self._client(client)
        r.set(self.queued_key(workflow_id, task_id), "1")
        self.enqueue_task(workflow_id, task_id, client=r)

    def clear_enqueue_guard(self, workflow_id: str, task_id: str, client=None) -> None:
        """Allow the guard to fire again, e.g. when a task is retried."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._client(client).delete(self.queued_key(workflow_id, task_id))

    # claiming a task: dequeue and lease in one step
    def claim(
        self,
        workflow_id: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        on_claim: Hook | None = None,
    ) -> str | None:
        """Take the next task off the queue and lease it, atomically.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        key = self.queue_key(workflow_id)

        def take(pipe):
            head = pipe.lindex(key, -1)
            if head is None:
                return None
            task_id = _decode(head)
            write = on_claim(pipe, task_id) if on_claim is not None else None
            pipe.multi()
            pipe.rpop(key)
            self.lease(workflow_id, task_id, lease_seconds, client=pipe)
            if write is not None:
                write(pipe)
            return task_id

        return self._redis.transaction(take, key, value_from_callable=True)

    def discard(self, workflow_id: str, task_id: str) -> None:
        """Drop every queued entry of a task, e.g. one whose state is gone."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._redis.lrem(self.queue_key(workflow_id), 0, task_id)

    # leases of running tasks
    def lease(
        self,
        workflow_id: str,
        task_id: str,
        seconds: float = DEFAULT_LEASE_SECONDS,
        now: float | None = None,
        client=None,
    ) -> float:
        """Hold (or renew) a task's lease; returns the new deadline."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if seconds <= 0:
            raise ValueError("seconds must be positive")

        deadline = (time.time() if now is None else now) + seconds
        self._client(client).zadd(self.processing_key(workflow_id), {task_id: deadline})
        return deadline

    def release_lease(self, workflow_id: str, task_id: str, client=None) -> None:
        """Drop a task's lease once it is no longer running."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._client(client).zrem(self.processing_key(workflow_id), task_id)

    def leased_tasks(self, workflow_id: str) -> list[str]:
        """Tasks currently held by a worker, earliest deadline first."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        return [
            _decode(t) for t in self._redis.zrange(self.processing_key(workflow_id), 0, -1)
        ]

    def reclaim_expired(
        self,
        workflow_id: str,
        now: float | None = None,
        on_reclaim: Hook | None = None,
    ) -> list[str]:
        """Re-queue tasks whose lease has expired (their worker is gone).
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        now = time.time() if now is None else now
        key = self.processing_key(workflow_id)
        return [
            task_id
            for task_id in map(_decode, self._redis.zrangebyscore(key, "-inf", now))
            if self._move_to_queue(workflow_id, key, task_id, now, on_reclaim)
        ]

    def _move_to_queue(
        self,
        workflow_id: str,
        source_key: str,
        task_id: str,
        due: float | None,
        hook: Hook | None,
    ) -> bool:
        """Move a task from a sorted set onto the queue; True if moved.
        """

        def move(pipe):
            score = pipe.zscore(source_key, task_id)
            if score is None or (due is not None and score > due):
                return False
            write = hook(pipe, task_id) if hook is not None else None
            pipe.multi()
            pipe.zrem(source_key, task_id)
            if write is not None:
                write(pipe)
            self.enqueue_task(workflow_id, task_id, client=pipe)
            return True

        return self._redis.transaction(move, source_key, value_from_callable=True)

    def mark_blocked(self, workflow_id: str, task_id: str, now: float | None = None) -> float:
        """Record when a task was *first* blocked; returns that moment.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        now = time.time() if now is None else now
        key = self.blocked_since_key(workflow_id)
        self._redis.hsetnx(key, task_id, now)
        return float(self._redis.hget(key, task_id))

    def increment_attempts(self, workflow_id: str, task_id: str) -> int:
        """Count one more deferral of a task; returns the new count (1, 2, …)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        return int(self._redis.hincrby(self.attempts_key(workflow_id), task_id, 1))

    def reset_attempts(self, workflow_id: str, task_id: str, client=None) -> None:
        """Forget a task's deferral count and start time once it has finished."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        r = self._client(client)
        r.hdel(self.attempts_key(workflow_id), task_id)
        r.hdel(self.blocked_since_key(workflow_id), task_id)

    def enqueue_task(self, workflow_id: str, task_id: str, client=None) -> None:
        """Add task to workflow queue (FIFO)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        self._client(client).lpush(self.queue_key(workflow_id), task_id)

    def dequeue_task(self, workflow_id: str, timeout: int = 0) -> str | None:
        """Remove and return next task from queue, without leasing it.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if timeout < 0:
            raise ValueError("timeout must be non-negative")

        if timeout == 0:
            task_id = self._redis.rpop(self.queue_key(workflow_id))
        else:
            result = self._redis.brpop(self.queue_key(workflow_id), timeout=timeout)
            task_id = result[1] if result is not None else None
        if task_id is None:
            return None
        return _decode(task_id)

    def schedule_task(
        self, workflow_id: str, task_id: str, delay: float, client=None
    ) -> None:
        """Park a task and make it eligible for the queue after delay seconds.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if delay < 0:
            raise ValueError("delay must be non-negative")

        self._client(client).zadd(
            self.delayed_key(workflow_id), {task_id: time.time() + delay}
        )

    def release_due_tasks(
        self,
        workflow_id: str,
        now: float | None = None,
        on_release: Hook | None = None,
    ) -> list[str]:
        """Move parked tasks whose delay has elapsed onto the queue."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        now = time.time() if now is None else now
        key = self.delayed_key(workflow_id)
        return [
            task_id
            for task_id in map(_decode, self._redis.zrangebyscore(key, "-inf", now))
            if self._move_to_queue(workflow_id, key, task_id, now, on_release)
        ]

    def release_tasks(
        self,
        workflow_id: str,
        task_ids: list[str],
        on_release: Hook | None = None,
    ) -> list[str]:
        """Move specific parked tasks onto the queue immediately.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        key = self.delayed_key(workflow_id)
        return [
            task_id
            for task_id in map(_decode, task_ids)
            if self._move_to_queue(workflow_id, key, task_id, None, on_release)
        ]

    def unschedule(self, workflow_id: str, task_id: str, client=None) -> None:
        """Forget a pending retry, e.g. because the task reached a final state."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._client(client).zrem(self.delayed_key(workflow_id), task_id)

    def delayed_tasks(self, workflow_id: str) -> list[str]:
        """Parked task ids, earliest release first."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        return [
            _decode(t) for t in self._redis.zrange(self.delayed_key(workflow_id), 0, -1)
        ]

    def peek_queue(self, workflow_id: str, count: int = 10) -> list[str]:
        """View tasks in queue without removing them."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if count < 0:
            raise ValueError("count must be non-negative")

        items = self._redis.lrange(self.queue_key(workflow_id), -count, -1)
        return [_decode(item) for item in reversed(items)]

    def queue_length(self, workflow_id: str) -> int:
        """Get number of tasks in queue."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        return self._redis.llen(self.queue_key(workflow_id))

    def clear_queue(self, workflow_id: str) -> None:
        """Remove all tasks from queue, including parked ones."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        keys = [
            self.queue_key(workflow_id),
            self.delayed_key(workflow_id),
            self.attempts_key(workflow_id),
            self.blocked_since_key(workflow_id),
            self.processing_key(workflow_id),
        ]
        keys += [
            _decode(k)
            for pattern in (f"inputs:{workflow_id}:*", f"queued:{workflow_id}:*")
            for k in self._redis.scan_iter(match=pattern, count=100)
        ]
        self._redis.delete(*keys)
