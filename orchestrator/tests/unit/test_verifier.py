"""The workflow verifier: quiet on healthy workflows, loud on damaged ones.
"""

import json

import fakeredis
import pytest

from models.data_reference import DataReference, Format, Protocol
from services.secrets import services_key_name
from services.state_store import RUNNING_WORKFLOWS_KEY, RedisStateStore, WorkflowNotFoundError
from services.task_queue import RedisTaskQueue
from services.verifier import CHECKS, WorkflowVerifier
from services.workflow_engine import WorkflowEngine
from tests.unit.test_workflow_engine import (
    create_chain_graph,
    create_fan_in_graph,
    create_single_node_graph,
)


@pytest.fixture(params=[False, True], ids=["bytes", "decoded"])
def redis_client(request):
    return fakeredis.FakeRedis(decode_responses=request.param)


@pytest.fixture
def engine(redis_client):
    return WorkflowEngine(RedisStateStore(redis_client), RedisTaskQueue(redis_client), redis_client)


@pytest.fixture
def verifier(engine):
    return WorkflowVerifier(engine)


def ref(name: str) -> DataReference:
    return DataReference(
        protocol=Protocol.FILE, uri=f"/data/{name}.csv", format=Format.CSV,
        checksum="sha256:" + "ab" * 32, size_bytes=10,
    )


def failing(verifier, workflow_id="wf-1") -> dict[str, list[str]]:
    return {r.name: r.problems for r in verifier.verify(workflow_id) if not r.passed}


def run_to_completion(engine, graph, redis_client=None):
    engine.initialize_workflow("wf-1", graph)
    if redis_client is not None:
        redis_client.set(services_key_name("wf-1"), "key")
    engine.start_workflow("wf-1", initial_inputs=[ref("initial")])
    while (task := engine.claim_task("wf-1")) is not None:
        engine.complete_task("wf-1", task.task_id, [ref(task.task_id)])


# healthy workflows pass every check
class TestHealthyWorkflows:
    def test_all_checks_are_reported(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        assert [r.name for r in verifier.verify("wf-1")] == list(CHECKS)

    def test_just_started(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_fan_in_graph())
        engine.start_workflow("wf-1")
        assert failing(verifier) == {}

    def test_mid_run(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_chain_graph())
        engine.start_workflow("wf-1")
        first = engine.claim_task("wf-1")
        engine.complete_task("wf-1", first.task_id, [ref("a")])
        engine.claim_task("wf-1")
        assert failing(verifier) == {}

    def test_deferred_and_reclaimed(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_chain_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")
        engine.record_blocked_attempt("wf-1", task.task_id)
        engine.blocked_age("wf-1", task.task_id)
        engine.defer_task("wf-1", task.task_id, "service down", delay=0)
        assert failing(verifier) == {}
        engine.release_due_tasks("wf-1")
        engine.claim_task("wf-1")
        engine.task_queue.lease("wf-1", task.task_id, seconds=1, now=0.0)
        engine.release_due_tasks("wf-1")
        assert failing(verifier) == {}

    def test_failed_and_resumed(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_chain_graph())
        engine.start_workflow("wf-1")
        task = engine.claim_task("wf-1")
        engine.fail_task("wf-1", task.task_id, "boom")
        assert failing(verifier) == {}
        engine.resume_workflow("wf-1")
        assert failing(verifier) == {}

    @pytest.mark.parametrize("graph", [create_chain_graph, create_fan_in_graph])
    def test_completed(self, engine, verifier, redis_client, graph):
        run_to_completion(engine, graph(), redis_client)
        assert engine.get_workflow_status("wf-1").status.value == "completed"
        assert failing(verifier) == {}

    def test_unknown_workflow(self, verifier):
        with pytest.raises(WorkflowNotFoundError):
            verifier.verify("wf-missing")

    def test_verifying_changes_nothing(self, engine, verifier, redis_client):
        run_to_completion(engine, create_fan_in_graph(), redis_client)
        before = {k: redis_client.dump(k) for k in redis_client.keys("*")}
        verifier.verify("wf-1")
        assert {k: redis_client.dump(k) for k in redis_client.keys("*")} == before


# each kind of damage is named by its check
class TestDamageIsFound:
    @pytest.fixture
    def done(self, engine, redis_client):
        """A completed chain A -> B -> C, and its task ids in order."""
        run_to_completion(engine, create_chain_graph(), redis_client)
        tasks = {t.node_key: t.task_id for t in engine.get_all_tasks("wf-1")}
        return [tasks["service-a:Process"], tasks["service-b:Process"], tasks["service-c:Process"]]

    def edit_task(self, engine, redis_client, task_id, **changes):
        store = engine.state_store
        task = store.get_task("wf-1", task_id).model_copy(update=changes)
        redis_client.set(store.task_key("wf-1", task_id), task.model_dump_json())

    def test_status_running_index(self, verifier, redis_client, done):
        redis_client.sadd(RUNNING_WORKFLOWS_KEY, "wf-1")
        assert list(failing(verifier)) == ["status"]

    def test_status_completed_with_an_unfinished_task(self, engine, verifier, redis_client, done):
        from models.state import TaskStatus

        self.edit_task(engine, redis_client, done[1], status=TaskStatus.FAILED, error="x")
        assert "status" in failing(verifier)

    def test_placement_lease_left_behind(self, engine, verifier, done):
        engine.task_queue.lease("wf-1", done[2])
        problems = failing(verifier)
        assert "completed but is queued 0 time(s), not parked and leased" in problems["placement"][0]
        assert "cleanup" in problems  # the leases key outlived the workflow too

    def test_placement_queued_twice(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_single_node_graph())
        engine.start_workflow("wf-1")
        (task_id,) = engine.task_queue.peek_queue("wf-1")
        engine.task_queue.enqueue_task("wf-1", task_id)
        assert "queued 2 time(s)" in failing(verifier)["placement"][0]

    def test_dependencies(self, engine, verifier):
        engine.initialize_workflow("wf-1", create_chain_graph())
        engine.start_workflow("wf-1")
        (first,) = engine.task_queue.peek_queue("wf-1")
        (second,) = engine.redis.smembers(engine.dependents_key("wf-1", first))
        second = second.decode() if isinstance(second, bytes) else second
        engine.redis.delete(engine.deps_key("wf-1", second))
        problems = failing(verifier)
        assert "waits for [], expected" in problems["dependencies"][0]
        # with nothing left to wait for it should also be queued, and is not
        assert list(problems) == ["placement", "dependencies"]

    def test_handover_altered_checksum(self, engine, verifier, redis_client, done):
        key = engine.task_queue.inputs_key("wf-1", done[1])
        payload = json.loads(redis_client.hget(key, done[0]))
        payload[0]["checksum"] = "sha256:" + "cd" * 32
        redis_client.hset(key, done[0], json.dumps(payload))
        problems = failing(verifier)
        assert "differ from what it produced" in problems["handover"][0]
        assert "inputs" in problems  

    def test_handover_lost(self, engine, verifier, redis_client, done):
        redis_client.hdel(engine.task_queue.inputs_key("wf-1", done[2]), done[1])
        assert "never reached" in failing(verifier)["handover"][0]

    def test_inputs_altered_after_claim(self, engine, verifier, redis_client, done):
        self.edit_task(engine, redis_client, done[1], input_refs=[ref("elsewhere")])
        assert list(failing(verifier)) == ["inputs"]

    def test_bookkeeping(self, engine, verifier, redis_client, done):
        redis_client.hset(engine.task_queue.attempts_key("wf-1"), done[0], 2)
        problems = failing(verifier)
        assert "bookkeeping" in problems
        assert "cleanup" in problems

    def test_history(self, engine, verifier, redis_client, done):
        self.edit_task(engine, redis_client, done[0], claims=0)
        assert list(failing(verifier)) == ["history"]

    def test_cleanup_services_key(self, verifier, redis_client, done):
        redis_client.set(services_key_name("wf-1"), "key")
        assert failing(verifier) == {"cleanup": ["completed, but its services key is still stored"]}
