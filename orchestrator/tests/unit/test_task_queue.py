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


class TestNonBlockingDequeue:
    """timeout=0 must not block (Redis BRPOP treats 0 as forever)."""

    def test_zero_timeout_on_empty_queue_returns_none(self, queue):
        assert queue.dequeue_task("wf-1", timeout=0) is None

    def test_zero_timeout_returns_fifo(self, queue):
        queue.enqueue_task("wf-1", "task-1")
        queue.enqueue_task("wf-1", "task-2")

        assert queue.dequeue_task("wf-1", timeout=0) == "task-1"
        assert queue.dequeue_task("wf-1", timeout=0) == "task-2"


class TestDelayedTasks:
    """Tests for parking tasks and releasing them later."""

    def test_scheduled_task_not_queued_immediately(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=60)

        assert queue.queue_length("wf-1") == 0
        assert queue.delayed_tasks("wf-1") == ["task-1"]

    def test_release_due_tasks_moves_only_due(self, queue):
        queue.schedule_task("wf-1", "due", delay=0)
        queue.schedule_task("wf-1", "later", delay=3600)

        released = queue.release_due_tasks("wf-1")

        assert released == ["due"]
        assert queue.dequeue_task("wf-1", timeout=1) == "due"
        assert queue.delayed_tasks("wf-1") == ["later"]

    def test_release_due_tasks_with_explicit_now(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=10)

        assert queue.release_due_tasks("wf-1") == []
        import time

        assert queue.release_due_tasks("wf-1", now=time.time() + 11) == ["task-1"]

    def test_release_tasks_ignores_unknown(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=3600)

        released = queue.release_tasks("wf-1", ["task-1", "not-parked"])

        assert released == ["task-1"]
        assert queue.queue_length("wf-1") == 1

    def test_release_is_exactly_once(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=0)

        assert queue.release_tasks("wf-1", ["task-1"]) == ["task-1"]
        assert queue.release_due_tasks("wf-1") == []
        assert queue.queue_length("wf-1") == 1

    def test_on_release_runs_before_enqueue(self, queue):
        seen = []
        queue.schedule_task("wf-1", "task-1", delay=0)

        queue.release_due_tasks(
            "wf-1", on_release=lambda t: seen.append((t, queue.queue_length("wf-1")))
        )

        assert seen == [("task-1", 0)]

    def test_rescheduling_does_not_duplicate(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=0)
        queue.schedule_task("wf-1", "task-1", delay=0)

        assert queue.release_due_tasks("wf-1") == ["task-1"]
        assert queue.queue_length("wf-1") == 1

    def test_negative_delay_raises(self, queue):
        with pytest.raises(ValueError, match="delay"):
            queue.schedule_task("wf-1", "task-1", delay=-1)

    def test_empty_ids_raise(self, queue):
        with pytest.raises(ValueError, match="workflow_id"):
            queue.schedule_task("", "task-1", delay=0)
        with pytest.raises(ValueError, match="task_id"):
            queue.schedule_task("wf-1", "", delay=0)
        with pytest.raises(ValueError, match="workflow_id"):
            queue.release_due_tasks("")

    def test_clear_queue_removes_parked_tasks(self, queue):
        queue.schedule_task("wf-1", "task-1", delay=3600)

        queue.clear_queue("wf-1")

        assert queue.delayed_tasks("wf-1") == []


class TestRetryAttempts:
    """Per-task deferral counts that drive the retry backoff."""

    def test_counts_up_from_one(self, queue):
        assert [queue.increment_attempts("wf-1", "task-1") for _ in range(3)] == [1, 2, 3]

    def test_counts_are_per_task(self, queue):
        queue.increment_attempts("wf-1", "task-1")

        assert queue.increment_attempts("wf-1", "task-2") == 1

    def test_reset_starts_over(self, queue):
        queue.increment_attempts("wf-1", "task-1")
        queue.increment_attempts("wf-1", "task-1")
        queue.reset_attempts("wf-1", "task-1")

        assert queue.increment_attempts("wf-1", "task-1") == 1

    def test_clear_queue_removes_counts(self, queue):
        queue.increment_attempts("wf-1", "task-1")
        queue.clear_queue("wf-1")

        assert queue.increment_attempts("wf-1", "task-1") == 1

    def test_ids_required(self, queue):
        with pytest.raises(ValueError, match="workflow_id"):
            queue.increment_attempts("", "task-1")
        with pytest.raises(ValueError, match="task_id"):
            queue.reset_attempts("wf-1", "")


class TestBlockedSince:
    """When a task was first blocked, for the maximum-blocked-time limit."""

    def test_first_call_records_now(self, queue):
        marked = queue.mark_blocked("wf-1", "task-1", now=1000.0)

        assert marked == 1000.0

    def test_later_calls_keep_the_first_time(self, queue):
        queue.mark_blocked("wf-1", "task-1", now=1000.0)

        assert queue.mark_blocked("wf-1", "task-1", now=5000.0) == 1000.0

    def test_per_task(self, queue):
        queue.mark_blocked("wf-1", "task-1", now=1000.0)

        assert queue.mark_blocked("wf-1", "task-2", now=5000.0) == 5000.0

    def test_reset_attempts_clears_it(self, queue):
        queue.mark_blocked("wf-1", "task-1", now=1000.0)
        queue.reset_attempts("wf-1", "task-1")

        assert queue.mark_blocked("wf-1", "task-1", now=5000.0) == 5000.0

    def test_clear_queue_clears_it(self, queue):
        queue.mark_blocked("wf-1", "task-1", now=1000.0)
        queue.clear_queue("wf-1")

        assert queue.mark_blocked("wf-1", "task-1", now=5000.0) == 5000.0

    def test_ids_required(self, queue):
        with pytest.raises(ValueError, match="workflow_id"):
            queue.mark_blocked("", "task-1")
        with pytest.raises(ValueError, match="task_id"):
            queue.mark_blocked("wf-1", "")


class TestInputs:
    """Tests for the per-producer inputs hash."""

    def test_inputs_empty_by_default(self, queue):
        """A task with no stored inputs has none."""
        assert queue.inputs("wf-1", "task-1") == []

    def test_add_and_read_inputs(self, queue):
        """Stored inputs come back."""
        queue.add_inputs("wf-1", "task-c", "task-a", '[{"uri": "a"}]')
        assert queue.inputs("wf-1", "task-c") == ['[{"uri": "a"}]']

    def test_producers_do_not_overwrite_each_other(self, queue):
        """Two producers of the same task keep both payloads."""
        queue.add_inputs("wf-1", "task-c", "task-a", "from-a")
        queue.add_inputs("wf-1", "task-c", "task-b", "from-b")
        assert queue.inputs("wf-1", "task-c") == ["from-a", "from-b"]

    def test_initial_inputs_come_first(self, queue):
        """Workflow inputs sort before any producing task."""
        queue.add_inputs("wf-1", "task-a", "task_zz", "from-task")
        queue.add_inputs("wf-1", "task-a", "initial", "from-submission")
        assert queue.inputs("wf-1", "task-a") == ["from-submission", "from-task"]

    def test_same_producer_twice_replaces(self, queue):
        """Re-running a producer replaces its own inputs, it does not add."""
        queue.add_inputs("wf-1", "task-c", "task-a", "first")
        queue.add_inputs("wf-1", "task-c", "task-a", "second")
        assert queue.inputs("wf-1", "task-c") == ["second"]

    def test_add_inputs_requires_ids(self, queue):
        """Missing identifiers are rejected."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.add_inputs("", "task-c", "task-a", "x")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.add_inputs("wf-1", "", "task-a", "x")
        with pytest.raises(ValueError, match="source is required"):
            queue.add_inputs("wf-1", "task-c", "", "x")

    def test_inputs_requires_ids(self, queue):
        """Reading inputs needs both identifiers."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.inputs("", "task-c")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.inputs("wf-1", "")


class TestClaimEnqueue:
    """Tests for the at-most-once enqueue guard."""

    def test_first_call_enqueues(self, queue):
        """The first claim queues the task."""
        assert queue.claim_enqueue("wf-1", "task-1") is True
        assert queue.queue_length("wf-1") == 1

    def test_second_call_does_not_enqueue(self, queue):
        """A second claim is refused, so the task is queued once."""
        queue.claim_enqueue("wf-1", "task-1")
        assert queue.claim_enqueue("wf-1", "task-1") is False
        assert queue.queue_length("wf-1") == 1

    def test_guard_is_per_task(self, queue):
        """Different tasks are independent."""
        assert queue.claim_enqueue("wf-1", "task-1") is True
        assert queue.claim_enqueue("wf-1", "task-2") is True
        assert queue.queue_length("wf-1") == 2

    def test_clearing_the_guard_allows_requeue(self, queue):
        """A retried task can be queued again."""
        queue.claim_enqueue("wf-1", "task-1")
        queue.clear_enqueue_guard("wf-1", "task-1")
        assert queue.claim_enqueue("wf-1", "task-1") is True
        assert queue.queue_length("wf-1") == 2

    def test_requires_ids(self, queue):
        """Missing identifiers are rejected."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.claim_enqueue("", "task-1")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.claim_enqueue("wf-1", "")
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.clear_enqueue_guard("", "task-1")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.clear_enqueue_guard("wf-1", "")


class TestLeases:
    """Tests for leases of running tasks."""

    def test_lease_records_the_task(self, queue):
        """A leased task is listed as being processed."""
        queue.lease("wf-1", "task-1", seconds=60)
        assert queue.leased_tasks("wf-1") == ["task-1"]

    def test_lease_returns_deadline(self, queue):
        """The lease deadline is now plus the lease time."""
        deadline = queue.lease("wf-1", "task-1", seconds=60, now=1000.0)
        assert deadline == 1060.0

    def test_lease_renews(self, queue):
        """Leasing again moves the deadline, it does not duplicate the task."""
        queue.lease("wf-1", "task-1", seconds=60, now=1000.0)
        queue.lease("wf-1", "task-1", seconds=60, now=2000.0)
        assert queue.leased_tasks("wf-1") == ["task-1"]
        assert queue.reclaim_expired("wf-1", now=1500.0) == []

    def test_release_lease(self, queue):
        """A finished task holds no lease."""
        queue.lease("wf-1", "task-1", seconds=60)
        queue.release_lease("wf-1", "task-1")
        assert queue.leased_tasks("wf-1") == []

    def test_release_unknown_lease_is_harmless(self, queue):
        """Releasing a lease that is not held does nothing."""
        queue.release_lease("wf-1", "task-1")
        assert queue.leased_tasks("wf-1") == []

    def test_leases_sorted_by_deadline(self, queue):
        """The task closest to expiry comes first."""
        queue.lease("wf-1", "later", seconds=60, now=1000.0)
        queue.lease("wf-1", "sooner", seconds=10, now=1000.0)
        assert queue.leased_tasks("wf-1") == ["sooner", "later"]

    def test_lease_rejects_non_positive_seconds(self, queue):
        """A lease must last."""
        with pytest.raises(ValueError, match="seconds must be positive"):
            queue.lease("wf-1", "task-1", seconds=0)

    def test_lease_requires_ids(self, queue):
        """Missing identifiers are rejected."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.lease("", "task-1")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.lease("wf-1", "")
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.release_lease("", "task-1")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.release_lease("wf-1", "")
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.leased_tasks("")


class TestReclaimExpired:
    """Tests for reclaiming tasks whose worker is gone."""

    def test_expired_lease_is_requeued(self, queue):
        """A task past its deadline goes back on the queue."""
        queue.lease("wf-1", "task-1", seconds=10, now=1000.0)
        assert queue.reclaim_expired("wf-1", now=2000.0) == ["task-1"]
        assert queue.queue_length("wf-1") == 1
        assert queue.leased_tasks("wf-1") == []

    def test_live_lease_is_left_alone(self, queue):
        """A task still within its lease is not touched."""
        queue.lease("wf-1", "task-1", seconds=600, now=1000.0)
        assert queue.reclaim_expired("wf-1", now=1100.0) == []
        assert queue.queue_length("wf-1") == 0
        assert queue.leased_tasks("wf-1") == ["task-1"]

    def test_reclaimed_once(self, queue):
        """A second pass does not queue the task again."""
        queue.lease("wf-1", "task-1", seconds=10, now=1000.0)
        queue.reclaim_expired("wf-1", now=2000.0)
        assert queue.reclaim_expired("wf-1", now=2000.0) == []
        assert queue.queue_length("wf-1") == 1

    def test_callback_runs_before_requeue(self, queue):
        """The caller can reset state before the task becomes claimable."""
        seen = []
        queue.lease("wf-1", "task-1", seconds=10, now=1000.0)
        queue.reclaim_expired("wf-1", now=2000.0, on_reclaim=seen.append)
        assert seen == ["task-1"]

    def test_reclaim_requires_workflow_id(self, queue):
        """Missing workflow id is rejected."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.reclaim_expired("")


class TestClearQueueRemovesSchedulingState:
    """clear_queue must not leave scheduling state behind."""

    def test_clears_inputs_guards_and_leases(self, queue, redis_client):
        """Deleting a workflow removes its inputs, guards and leases."""
        queue.add_inputs("wf-1", "task-1", "initial", "payload")
        queue.claim_enqueue("wf-1", "task-1")
        queue.lease("wf-1", "task-1", seconds=60)

        queue.clear_queue("wf-1")

        assert queue.inputs("wf-1", "task-1") == []
        assert queue.leased_tasks("wf-1") == []
        assert queue.queue_length("wf-1") == 0
        assert redis_client.keys("inputs:wf-1:*") == []
        assert redis_client.keys("queued:wf-1:*") == []

    def test_leaves_other_workflows_alone(self, queue):
        """Only the given workflow is cleared."""
        queue.add_inputs("wf-1", "task-1", "initial", "one")
        queue.add_inputs("wf-2", "task-1", "initial", "two")
        queue.clear_queue("wf-1")
        assert queue.inputs("wf-2", "task-1") == ["two"]


class TestUnschedule:
    """A task in a final state must not come back from the delayed set."""

    def test_removes_a_pending_retry(self, queue):
        """Unscheduling drops the task from the delayed set."""
        queue.schedule_task("wf-1", "task-1", delay=60)
        queue.unschedule("wf-1", "task-1")
        assert queue.delayed_tasks("wf-1") == []

    def test_unknown_task_is_harmless(self, queue):
        """Unscheduling a task that is not waiting does nothing."""
        queue.unschedule("wf-1", "task-1")
        assert queue.delayed_tasks("wf-1") == []

    def test_requires_ids(self, queue):
        """Missing identifiers are rejected."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            queue.unschedule("", "task-1")
        with pytest.raises(ValueError, match="task_id is required"):
            queue.unschedule("wf-1", "")
