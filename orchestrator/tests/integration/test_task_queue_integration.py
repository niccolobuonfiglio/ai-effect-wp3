"""Integration tests for RedisTaskQueue with real Redis."""

import threading
import time

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from services.task_queue import RedisTaskQueue


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as container:
        yield container


@pytest.fixture
def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=False,
    )
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def queue(redis_client):
    return RedisTaskQueue(redis_client)


class TestEnqueueDequeue:
    """Integration tests for enqueue/dequeue operations."""

    def test_enqueue_dequeue_cycle(self, queue):
        """Full enqueue-dequeue cycle with real Redis."""
        queue.enqueue_task("wf-int-1", "task-1")
        queue.enqueue_task("wf-int-1", "task-2")
        queue.enqueue_task("wf-int-1", "task-3")

        assert queue.queue_length("wf-int-1") == 3

        assert queue.dequeue_task("wf-int-1", timeout=1) == "task-1"
        assert queue.dequeue_task("wf-int-1", timeout=1) == "task-2"
        assert queue.dequeue_task("wf-int-1", timeout=1) == "task-3"

        assert queue.queue_length("wf-int-1") == 0

    def test_dequeue_blocks_and_returns_none(self, queue):
        """Dequeue blocks and returns None when timeout expires."""
        start = time.time()
        result = queue.dequeue_task("wf-empty", timeout=1)
        elapsed = time.time() - start

        assert result is None
        assert elapsed >= 0.9  # Allow small timing variance


class TestConcurrentAccess:
    """Integration tests for concurrent access."""

    def test_concurrent_dequeue(self, redis_container):
        """Multiple workers dequeue tasks without duplicates."""
        client = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        queue = RedisTaskQueue(client)

        for i in range(10):
            queue.enqueue_task("wf-concurrent", f"task-{i}")

        results = []
        lock = threading.Lock()

        def worker():
            worker_client = Redis(
                host=redis_container.get_container_host_ip(),
                port=redis_container.get_exposed_port(6379),
                decode_responses=False,
            )
            worker_queue = RedisTaskQueue(worker_client)

            while True:
                task_id = worker_queue.dequeue_task("wf-concurrent", timeout=1)
                if task_id is None:
                    break
                with lock:
                    results.append(task_id)

            worker_client.close()

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        client.close()

        # All tasks processed exactly once
        assert len(results) == 10
        assert len(set(results)) == 10


class TestDataPersistence:
    """Integration tests for data persistence."""

    def test_queue_survives_reconnect(self, redis_container):
        """Queue data persists across connections."""
        client1 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        queue1 = RedisTaskQueue(client1)

        queue1.enqueue_task("wf-persist", "task-1")
        queue1.enqueue_task("wf-persist", "task-2")
        client1.close()

        client2 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        queue2 = RedisTaskQueue(client2)

        assert queue2.queue_length("wf-persist") == 2
        assert queue2.dequeue_task("wf-persist", timeout=1) == "task-1"
        client2.close()
