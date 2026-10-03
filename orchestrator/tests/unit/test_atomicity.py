"""State transitions are all-or-nothing and do not lose concurrent updates.
"""

from unittest.mock import patch

import fakeredis
import pytest

from models.data_reference import DataReference, Format, Protocol
from models.state import TaskStatus, WorkflowStatus
from services.state_store import RUNNING_WORKFLOWS_KEY, RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine
from tests.unit.test_workflow_engine import (
    create_chain_graph,
    create_fan_in_graph,
    create_single_node_graph,
)


class Crash(Exception):
    """Stands in for a worker dying mid-transition."""


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()


@pytest.fixture
def store(redis_client):
    return RedisStateStore(redis_client)


@pytest.fixture
def queue(redis_client):
    return RedisTaskQueue(redis_client)


@pytest.fixture
def engine(store, queue, redis_client):
    return WorkflowEngine(store, queue, redis_client)


def ref(name: str) -> DataReference:
    return DataReference(protocol=Protocol.S3, uri=f"s3://b/{name}", format=Format.JSON)


def running_ids(redis_client) -> set[str]:
    return {m.decode() for m in redis_client.smembers(RUNNING_WORKFLOWS_KEY)}


class TestCrashLeavesOldState:
    def test_crash_while_claiming(self, engine, store, queue):
        """The task stays queued and pending: no lease, no RUNNING status."""
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        (task_id,) = queue.peek_queue("wf-1")

        with patch.object(store, "write_task", side_effect=Crash):
            with pytest.raises(Crash):
                engine.claim_task("wf-1")

        assert queue.peek_queue("wf-1") == [task_id]
        assert queue.leased_tasks("wf-1") == []
        assert store.get_task("wf-1", task_id).status == TaskStatus.PENDING
        assert engine.claim_task("wf-1").task_id == task_id

    def test_crash_while_completing(self, engine, store, queue):
        """Neither the completion nor the hand-over to the next task applies."""
        engine.initialize_workflow("wf-1", create_chain_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")

        with patch.object(queue, "enqueue_once", side_effect=Crash):
            with pytest.raises(Crash):
                engine.complete_task("wf-1", task.task_id, output_refs=[ref("a")])

        assert store.get_task("wf-1", task.task_id).status == TaskStatus.RUNNING
        assert queue.leased_tasks("wf-1") == [task.task_id]
        assert queue.queue_length("wf-1") == 0
        engine.complete_task("wf-1", task.task_id, output_refs=[ref("a")])
        assert queue.queue_length("wf-1") == 1

    def test_crash_while_completing_last_task(self, engine, store, redis_client):
        """The workflow is not left completed with a running task, or vice versa."""
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")

        with patch.object(store, "write_workflow", side_effect=Crash):
            with pytest.raises(Crash):
                engine.complete_task("wf-1", task.task_id)

        assert store.get_task("wf-1", task.task_id).status == TaskStatus.RUNNING
        assert store.get_workflow("wf-1").status == WorkflowStatus.RUNNING
        assert running_ids(redis_client) == {"wf-1"}

    def test_crash_while_failing(self, engine, store, queue):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")

        with patch.object(store, "write_workflow", side_effect=Crash):
            with pytest.raises(Crash):
                engine.fail_task("wf-1", task.task_id, "boom")

        assert store.get_task("wf-1", task.task_id).status == TaskStatus.RUNNING
        assert queue.leased_tasks("wf-1") == [task.task_id]

    def test_crash_while_starting(self, engine, store, queue, redis_client):
        """A workflow is never RUNNING with nothing queued."""
        engine.initialize_workflow("wf-1", create_single_node_graph())

        with patch.object(queue, "enqueue_once", side_effect=Crash):
            with pytest.raises(Crash):
                engine.start_workflow("wf-1")

        assert store.get_workflow("wf-1").status == WorkflowStatus.PENDING
        assert running_ids(redis_client) == set()
        assert queue.queue_length("wf-1") == 0

    def test_crash_while_initializing(self, engine, store, redis_client):
        with patch.object(store, "write_task", side_effect=Crash):
            with pytest.raises(Crash):
                engine.initialize_workflow("wf-1", create_chain_graph())

        assert redis_client.keys("*") == []

    def test_crash_while_reclaiming(self, engine, store, queue):
        """A reclaimed task is either still leased or back in the queue."""
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")
        queue.lease("wf-1", task.task_id, seconds=1, now=0.0)  # long expired

        with patch.object(store, "write_task", side_effect=Crash):
            with pytest.raises(Crash):
                engine.release_due_tasks("wf-1")

        assert queue.leased_tasks("wf-1") == [task.task_id]
        assert queue.queue_length("wf-1") == 0
        assert engine.release_due_tasks("wf-1") == 1
        assert queue.peek_queue("wf-1") == [task.task_id]
        assert store.get_task("wf-1", task.task_id).status == TaskStatus.PENDING


class TestConcurrentWritersRetry:
    def test_task_update_keeps_concurrent_change(self, store, redis_client):
        """A write between our read and EXEC is not overwritten."""
        store.create_workflow("wf-1")
        store.create_task("wf-1", "t1", "svc:Op")
        other = RedisStateStore(redis_client)
        original = store.with_task_status
        interfered = []

        def interleave(*args, **kwargs):
            if not interfered:
                interfered.append(True)
                other.update_task_status("wf-1", "t1", TaskStatus.RUNNING, input_refs=[ref("in")])
            return original(*args, **kwargs)

        with patch.object(store, "with_task_status", side_effect=interleave):
            task = store.update_task_status(
                "wf-1", "t1", TaskStatus.COMPLETED, output_refs=[ref("out")]
            )

        assert task.status == TaskStatus.COMPLETED
        assert [r.uri for r in task.input_refs] == ["s3://b/in"]
        assert [r.uri for r in task.output_refs] == ["s3://b/out"]

    def test_join_completing_with_its_last_sibling(self, engine, store, queue, redis_client):
        """B completes while A's completion is in flight: C is queued once."""
        engine.initialize_workflow("wf-1", create_fan_in_graph())
        engine.start_workflow("wf-1")
        a = engine.claim_task("wf-1")
        b = engine.claim_task("wf-1")
        other = WorkflowEngine(
            RedisStateStore(redis_client), RedisTaskQueue(redis_client), redis_client
        )
        original = store.with_task_status
        interfered = []

        def interleave(state, status, **kwargs):
            if status == TaskStatus.COMPLETED and not interfered:
                interfered.append(True)
                other.complete_task("wf-1", b.task_id, output_refs=[ref("b")])
            return original(state, status, **kwargs)

        with patch.object(store, "with_task_status", side_effect=interleave):
            engine.complete_task("wf-1", a.task_id, output_refs=[ref("a")])

        assert queue.queue_length("wf-1") == 1
        join = engine.claim_task("wf-1")
        assert sorted(r.uri for r in join.input_refs) == ["s3://b/a", "s3://b/b"]


class TestRunningIndex:
    """``workflows:running`` follows the workflow status in every transition."""

    def test_lifecycle(self, engine, redis_client):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        assert running_ids(redis_client) == set()

        engine.start_workflow("wf-1")
        assert running_ids(redis_client) == {"wf-1"}

        task = engine.claim_task("wf-1")
        engine.fail_task("wf-1", task.task_id, "boom")
        assert running_ids(redis_client) == set()

        engine.resume_workflow("wf-1")
        assert running_ids(redis_client) == {"wf-1"}

        task = engine.claim_task("wf-1")
        engine.complete_task("wf-1", task.task_id)
        assert running_ids(redis_client) == set()

    def test_retry_task_reindexes(self, engine, redis_client):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")
        engine.fail_task("wf-1", task.task_id, "boom")

        engine.retry_task("wf-1", task.task_id)
        assert running_ids(redis_client) == {"wf-1"}

    def test_delete_unindexes(self, engine, store, redis_client):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")

        store.delete_workflow("wf-1")
        assert running_ids(redis_client) == set()


class TestOrphanedQueueEntry:
    def test_claim_drops_task_without_state(self, engine, queue):
        """A queued id with no task document is dropped instead of blocking."""
        queue.enqueue_task("wf-1", "ghost")

        assert engine.claim_task("wf-1") is None
        assert queue.queue_length("wf-1") == 0
        assert queue.leased_tasks("wf-1") == []
