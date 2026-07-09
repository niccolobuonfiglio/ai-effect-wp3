"""Unit tests for RedisTaskQueue."""

import pytest
import fakeredis

from services.task_queue import RedisTaskQueue


@pytest.fixture
def redis_client():
    """Create a fakeredis client."""
    return fakeredis.FakeRedis()


@pytest.fixture
def queue(redis_client):
    """Create a RedisTaskQueue instance."""
    return RedisTaskQueue(redis_client)


class TestRedisTaskQueueInit:
    """Tests for RedisTaskQueue initialization."""

    def test_init_with_valid_client(self, redis_client):
        """Queue initializes with valid Redis client."""
        queue = RedisTaskQueue(redis_client)
        assert queue is not None

    def test_init_with_none_client_raises(self):
        """Queue raises ValueError when client is None."""
        with pytest.raises(ValueError, match="redis_client is required"):
            RedisTaskQueue(None)


class TestEnqueueTask:
    """Tests for enqueue_task method."""

    def test_enqueue_task(self, queue):
        """Task is added to queue."""
        queue.enqueue_task("wf-1", "task-1")
        assert queue.queue_length("wf-1") == 1

    def test_enqueue_multiple_tasks(self, queue):
        """Multiple tasks are added in order."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")
        queue.enqueue_task("wf-1", "task-3")
        assert queue.queue_length("wf-1") == 3

    def test_enqueue_empty_workflow_id_raises(self, queue):
        """Enqueue raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.enqueue_task("", "task-1")

    def test_enqueue_empty_task_id_raises(self, queue):
        """Enqueue raises ValueError for empty task_id."""
        with pytest.raises(ValueError, match="task_id is required"):
            queue.enqueue_task("wf-1", "")


class TestDequeueTask:
    """Tests for dequeue_task method."""

    def test_dequeue_task(self, queue):
        """Task is removed from queue."""
        queue.enqueue_task("wf-1", "task-1")
        task_id = queue.dequeue_task("wf-1", timeout=1)
        assert task_id == "task-1"
        assert queue.queue_length("wf-1") == 0

    def test_dequeue_empty_queue_returns_none(self, queue):
        """Dequeue from empty queue returns None after timeout."""
        task_id = queue.dequeue_task("wf-1", timeout=1)
        assert task_id is None

    def test_dequeue_fifo_order(self, queue):
        """Tasks are dequeued in FIFO order."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")
        queue.enqueue_task("wf-1", "task-3")

        assert queue.dequeue_task("wf-1", timeout=1) == "task-1"
        assert queue.dequeue_task("wf-1", timeout=1) == "task-2"
        assert queue.dequeue_task("wf-1", timeout=1) == "task-3"

    def test_dequeue_empty_workflow_id_raises(self, queue):
        """Dequeue raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.dequeue_task("", timeout=1)

    def test_dequeue_negative_timeout_raises(self, queue):
        """Dequeue raises ValueError for negative timeout."""
        with pytest.raises(ValueError, match="timeout must be non-negative"):
            queue.dequeue_task("wf-1", timeout=-1)


class TestPeekQueue:
    """Tests for peek_queue method."""

    def test_peek_queue(self, queue):
        """Peek returns tasks without removing them."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")

        tasks = queue.peek_queue("wf-1")
        assert tasks == ["task-1", "task-2"]
        assert queue.queue_length("wf-1") == 2

    def test_peek_empty_queue(self, queue):
        """Peek on empty queue returns empty list."""
        tasks = queue.peek_queue("wf-1")
        assert tasks == []

    def test_peek_with_count(self, queue):
        """Peek returns limited number of tasks."""
        for i in range(5):
            queue.enqueue_task("wf-1", f"task-{i}")

        tasks = queue.peek_queue("wf-1", count=3)
        assert tasks == ["task-0", "task-1", "task-2"]

    def test_peek_empty_workflow_id_raises(self, queue):
        """Peek raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.peek_queue("")

    def test_peek_negative_count_raises(self, queue):
        """Peek raises ValueError for negative count."""
        with pytest.raises(ValueError, match="count must be non-negative"):
            queue.peek_queue("wf-1", count=-1)


class TestQueueLength:
    """Tests for queue_length method."""

    def test_queue_length(self, queue):
        """Queue length returns correct count."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")
        assert queue.queue_length("wf-1") == 2

    def test_queue_length_empty(self, queue):
        """Empty queue has length 0."""
        assert queue.queue_length("wf-1") == 0

    def test_queue_length_empty_workflow_id_raises(self, queue):
        """Queue length raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.queue_length("")


class TestClearQueue:
    """Tests for clear_queue method."""

    def test_clear_queue(self, queue):
        """Clear removes all tasks."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")
        queue.clear_queue("wf-1")
        assert queue.queue_length("wf-1") == 0

    def test_clear_empty_queue(self, queue):
        """Clear on empty queue does not raise."""
        queue.clear_queue("wf-1")
        assert queue.queue_length("wf-1") == 0

    def test_clear_empty_workflow_id_raises(self, queue):
        """Clear raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.clear_queue("")


class TestWorkflowIsolation:
    """Tests for workflow isolation."""

    def test_workflow_isolation(self, queue):
        """Tasks in different workflows are isolated."""
        queue.enqueue_task("wf-1", "task-a")
        queue.enqueue_task("wf-2", "task-b")

        assert queue.queue_length("wf-1") == 1
        assert queue.queue_length("wf-2") == 1

        assert queue.dequeue_task("wf-1", timeout=1) == "task-a"
        assert queue.dequeue_task("wf-2", timeout=1) == "task-b"

    def test_clear_does_not_affect_other_workflows(self, queue):
        """Clearing one workflow does not affect others."""
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-2", "task-2")

        queue.clear_queue("wf-1")

        assert queue.queue_length("wf-1") == 0
        assert queue.queue_length("wf-2") == 1
