"""Property-based test of the workflow engine as a state machine.
"""

import os
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import permutations
from unittest.mock import patch

import fakeredis
import pytest
from hypothesis import event, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from redis import Redis

from models.data_reference import DataReference, Format, Protocol
from models.graph import ExecutionGraph
from models.state import TaskState, TaskStatus, WorkflowStatus
from services.secrets import services_key_name
from services.state_store import RUNNING_WORKFLOWS_KEY, RedisStateStore
from services.task_queue import RedisTaskQueue
from services.verifier import WorkflowVerifier
from services.workflow_engine import (
    INITIAL_INPUTS,
    SOURCE_NODE_KEY,
    NotFailedError,
    WorkflowEngine,
)
from tests.unit.test_workflow_engine import create_graph_node

WF = "wf-1"
BYSTANDER = "wf-10"

MAX_TASKS = 6
DUE_NOW, NOT_DUE = 0, 3600

PENDING, RUNNING, BLOCKED = TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.BLOCKED
COMPLETED, FAILED = TaskStatus.COMPLETED, TaskStatus.FAILED

dependency_graphs = st.integers(1, MAX_TASKS).flatmap(
    lambda size: st.tuples(
        *(st.frozensets(st.integers(0, max(0, i - 1)), max_size=i) for i in range(size))
    )
)
outcomes = st.sampled_from(["complete", "complete", "complete", "fail"])


def dies_at(*writes: str):
    """Mostly no crash; otherwise the write at which the worker dies.
    """
    return st.sampled_from([None, None, None, *writes])


class Crash(Exception):
    """Stands in for a worker dying mid-transition."""


@dataclass
class Task:
    """What the model knows about one task."""

    node_key: str
    service: str
    predecessors: frozenset[str]
    status: TaskStatus = PENDING
    produced: bool = False  # completed with an output
    due: bool = False  # blocked, and its retry delay has elapsed
    lease_expired: bool = False  # running, but its worker is gone
    attempts: int = 0  # deferrals since it last ended or was retried
    # its history, which the engine keeps on the task and never resets
    claims: int = 0
    reclaims: int = 0
    deferrals: int = 0
    retries: int = 0


def ref(name: str) -> DataReference:
    return DataReference(protocol=Protocol.S3, uri=f"s3://b/{name}", format=Format.JSON)


class EngineMachine(RuleBasedStateMachine):
    """The engine on fakeredis; a subclass may ``connect`` to a real Redis."""

    def __init__(self):
        super().__init__()
        self._server = fakeredis.FakeServer()
        self.redis = self.connect()
        self.store = RedisStateStore(self.redis)
        self.queue = RedisTaskQueue(self.redis)
        self.engine = WorkflowEngine(self.store, self.queue, self.redis)
        self.verifier = WorkflowVerifier(self.engine)
        # A second worker, with its own connection, for the races.
        other = self.connect()
        self.other = WorkflowEngine(RedisStateStore(other), RedisTaskQueue(other), other)

        self.tasks: dict[str, Task] = {}
        self.workflow: WorkflowStatus | None = None
        self.initial_inputs = False
        self.bystander: dict = {}
        self.checked_idle: tuple | None = None

    def connect(self) -> Redis:
        """A new connection to this run's own, empty database."""
        return fakeredis.FakeRedis(server=self._server, decode_responses=True)

    # reading Redis and the model
    def snapshot(self, workflow_id: str = WF, index: bool = True) -> dict:
        """Every key of a workflow and its content, and the running index."""
        keys = sorted(
            key
            for key in self.redis.keys("*")
            if workflow_id in key.split(":") or (index and key == RUNNING_WORKFLOWS_KEY)
        )
        pipe = self.redis.pipeline(transaction=False)
        for key in keys:
            pipe.type(key)
        for key, kind in zip(keys, pipe.execute()):
            if kind == "string":
                pipe.get(key)
            elif kind == "hash":
                pipe.hgetall(key)
            elif kind == "set":
                pipe.smembers(key)
            elif kind == "zset":
                pipe.zrange(key, 0, -1, withscores=True)
            else:
                pipe.lrange(key, 0, -1)
        return dict(zip(keys, pipe.execute()))

    def having(self, *statuses: TaskStatus) -> list[str]:
        return sorted(t for t, task in self.tasks.items() if task.status in statuses)

    def queued(self) -> list[str]:
        """Tasks the model expects on the queue: pending with nothing to wait for."""
        return sorted(
            t
            for t, task in self.tasks.items()
            if task.status == PENDING
            and all(self.tasks[p].status == COMPLETED for p in task.predecessors)
        )

    def due(self) -> tuple[str, ...]:
        """Tasks the next poll re-queues: retry delay elapsed, or lease expired."""
        return tuple(
            t
            for t, task in sorted(self.tasks.items())
            if (task.status == BLOCKED and task.due)
            or (task.status == RUNNING and task.lease_expired)
        )

    def expired(self) -> list[str]:
        return [t for t in self.having(RUNNING) if self.tasks[t].lease_expired]

    def held(self) -> list[str]:
        return [t for t in self.having(RUNNING) if not self.tasks[t].lease_expired]

    # crash injection
    @contextmanager
    def transition(self, crash: str | None, one_per_task: tuple[str, ...] | None = None):
        """Run an engine call that may die at the write named ``crash``.
        """
        if crash is None:
            yield
            return
        before = self.snapshot()
        owner = self.store if crash.startswith("write_") else self.queue
        try:
            with patch.object(owner, crash, side_effect=Crash):
                yield
        except Crash:
            event(f"crashed in {crash}")
            if one_per_task is None:
                assert self.snapshot() == before, f"a crash in {crash} left a partial change"
                return
            for task_id in one_per_task:
                status = self.store.get_task(WF, task_id).status
                assert status in (self.tasks[task_id].status, PENDING)
                if status == PENDING:
                    self._requeued(task_id)

    # model updates
    def _claimed(self, claimed: TaskState | None) -> None:
        queued = self.queued()
        if not queued:
            assert claimed is None
            return
        assert claimed is not None, f"nothing claimed although {queued} are queued"
        assert claimed.task_id in queued
        assert claimed.status == RUNNING
        task = self.tasks[claimed.task_id]
        expected = [
            self.tasks[p].node_key for p in task.predecessors if self.tasks[p].produced
        ]
        if self.initial_inputs and not task.predecessors:
            expected.append(INITIAL_INPUTS)
        assert sorted(r.metadata[SOURCE_NODE_KEY] for r in claimed.input_refs) == sorted(expected)
        task.status, task.lease_expired = RUNNING, False
        task.claims += 1

    def _requeued(self, task_id: str) -> None:
        task = self.tasks[task_id]
        if task.status == RUNNING:  # only a reclaim takes a running task back
            task.reclaims += 1
        task.status, task.due, task.lease_expired = PENDING, False, False

    def _finish(self, engine: WorkflowEngine, task_id: str, outcome: str, produce: bool) -> None:
        """Complete or fail a task through ``engine`` and update the model."""
        task = self.tasks[task_id]
        if outcome == "complete":
            done = engine.complete_task(WF, task_id, [ref(task_id)] if produce else None)
            assert done.status == COMPLETED
            task.status, task.produced = COMPLETED, produce
            if all(t.status == COMPLETED for t in self.tasks.values()):
                self.workflow = WorkflowStatus.COMPLETED
        else:
            failed = engine.fail_task(WF, task_id, "boom")
            assert failed.status == FAILED
            task.status = FAILED
            self.workflow = WorkflowStatus.FAILED
        task.attempts, task.due, task.lease_expired = 0, False, False

    def _retried(self, task_id: str) -> None:
        self._requeued(task_id)
        self.tasks[task_id].attempts = 0
        self.tasks[task_id].retries += 1
        self.workflow = WorkflowStatus.RUNNING

    # submission
    @initialize(dependencies=dependency_graphs, initial_inputs=st.booleans())
    def submit(self, dependencies, initial_inputs):
        graph = ExecutionGraph()
        nodes = [create_graph_node("svc-1" + "0" * i) for i in range(len(dependencies))]
        for node, before in zip(nodes, dependencies):
            for i in before:
                nodes[i].next_nodes.append(node)
                node.dependencies.append(nodes[i])
            graph.add_node(node)
            if not before:
                graph.start_nodes.append(node)

        inputs = [ref("initial")] if initial_inputs else None
        for workflow_id in (WF, BYSTANDER):
            self.redis.set(services_key_name(workflow_id), "key")
            self.engine.initialize_workflow(workflow_id, graph)
            self.engine.start_workflow(workflow_id, initial_inputs=inputs)

        ids = self.redis.hgetall(f"graph:{WF}")
        self.tasks = {
            ids[node.key]: Task(
                node_key=node.key,
                service=node.node.container_name,
                predecessors=frozenset(ids[nodes[i].key] for i in before),
            )
            for node, before in zip(nodes, dependencies)
        }
        self.workflow = WorkflowStatus.RUNNING
        self.initial_inputs = initial_inputs
        self.bystander = self.snapshot(BYSTANDER, index=False)

    # what workers do
    @precondition(lambda self: self.queued())
    @rule(crash=dies_at("write_task", "lease"))
    def claim(self, crash):
        with self.transition(crash):
            self._claimed(self.engine.claim_task(WF))

    @precondition(lambda self: self.having(RUNNING))
    @rule(
        data=st.data(),
        produce=st.booleans(),
        crash=dies_at(
            "write_task", "write_workflow", "enqueue_task", "release_lease",
            "reset_attempts", "add_inputs", "unschedule",
        ),
    )
    def complete(self, data, produce, crash):
        task_id = data.draw(st.sampled_from(self.having(RUNNING)))
        with self.transition(crash):
            self._finish(self.engine, task_id, "complete", produce)

    @precondition(lambda self: self.having(RUNNING, BLOCKED))
    @rule(
        data=st.data(),
        crash=dies_at("write_task", "write_workflow", "release_lease", "reset_attempts", "unschedule"),
    )
    def fail(self, data, crash):
        """A task fails, or gives up after being blocked for too long."""
        task_id = data.draw(st.sampled_from(self.having(RUNNING, BLOCKED)))
        with self.transition(crash):
            self._finish(self.engine, task_id, "fail", False)

    @precondition(lambda self: self.having(RUNNING))
    @rule(
        data=st.data(),
        delay=st.sampled_from([DUE_NOW, NOT_DUE]),
        crash=dies_at("write_task", "release_lease", "schedule_task"),
    )
    def defer(self, data, delay, crash):
        """The task's service is unavailable: count the attempt, then park it."""
        task_id = data.draw(st.sampled_from(self.having(RUNNING)))
        task = self.tasks[task_id]
        assert self.engine.record_blocked_attempt(WF, task_id) == task.attempts + 1
        task.attempts += 1
        assert self.engine.blocked_age(WF, task_id) >= 0
        with self.transition(crash):
            blocked = self.engine.defer_task(WF, task_id, "service down", delay)
            assert blocked.status == BLOCKED
            task.status, task.due, task.lease_expired = BLOCKED, delay == DUE_NOW, False
            task.deferrals += 1

    @precondition(lambda self: self.held())
    @rule(data=st.data())
    def worker_disappears(self, data):
        """A lease runs out because the worker holding the task is gone."""
        task_id = data.draw(st.sampled_from(self.held()))
        self.queue.lease(WF, task_id, seconds=1, now=0.0)
        self.tasks[task_id].lease_expired = True

    @precondition(lambda self: self.expired())
    @rule(data=st.data())
    def worker_was_only_slow(self, data):
        """The worker renews an expired lease before anyone took the task back."""
        task_id = data.draw(st.sampled_from(self.expired()))
        self.engine.renew_lease(WF, task_id)
        self.tasks[task_id].lease_expired = False

    @precondition(lambda self: self.due())
    @rule(crash=dies_at("write_task", "enqueue_task"))
    def poll(self, crash):
        """Re-queue blocked tasks that are due and tasks whose lease expired."""
        due = self.due()
        with self.transition(crash, one_per_task=due):
            assert self.engine.release_due_tasks(WF) == len(due)
            for task_id in due:
                self._requeued(task_id)

    # what operators do
    @precondition(lambda self: self.having(BLOCKED))
    @rule(data=st.data(), crash=dies_at("write_task", "enqueue_task"))
    def recover_service(self, data, crash):
        """Blocked tasks of a recovered service run at once, if the workflow runs."""
        task_id = data.draw(st.sampled_from(self.having(BLOCKED)))
        resumes = self.workflow == WorkflowStatus.RUNNING
        with self.transition(crash, one_per_task=(task_id,)):
            released = self.engine.resume_blocked_tasks(WF, self.tasks[task_id].service)
            assert released == int(resumes)
            if resumes:
                self._requeued(task_id)

    @precondition(lambda self: self.having(FAILED))
    @rule(
        data=st.data(),
        crash=dies_at("write_task", "write_workflow", "reset_attempts", "enqueue_task"),
    )
    def retry_task(self, data, crash):
        task_id = data.draw(st.sampled_from(self.having(FAILED)))
        with self.transition(crash):
            assert self.engine.retry_task(WF, task_id).status == PENDING
            self._retried(task_id)

    @precondition(lambda self: self.workflow == WorkflowStatus.FAILED)
    @rule(crash=dies_at("write_task", "write_workflow", "reset_attempts", "enqueue_task"))
    def resume_workflow(self, crash):
        failed = self.having(FAILED)
        with self.transition(crash):
            assert sorted(self.engine.resume_workflow(WF)) == failed
            for task_id in failed:
                self._retried(task_id)

    @precondition(lambda self: self.workflow == WorkflowStatus.COMPLETED)
    @rule()
    def nothing_more_can_happen(self):
        """A completed workflow offers no step; the properties still hold."""

    def stalled(self) -> bool:
        """Running, but nothing queued, parked or leased (docs/STALLS.md)."""
        return (
            self.workflow == WorkflowStatus.RUNNING
            and not self.queued()
            and not self.having(RUNNING, BLOCKED)
        )

    @precondition(lambda self: self.workflow == WorkflowStatus.RUNNING and not self.queued())
    @rule(crash=dies_at("write_workflow"))
    def watchdog(self, crash):
        """A worker's idle poll: a stalled workflow is failed, nothing else changes.

        Drawn only where the answer is not trivial: running, with nothing on
        the queue. Then only leased or parked work tells a busy workflow from
        a stalled one.
        """
        stalled, failed = self.stalled(), self.having(FAILED)
        with self.transition(crash):
            result = self.engine.fail_if_stalled(WF)
            if not stalled:
                assert result is None, f"not stalled, but the watchdog answered {result}"
            else:
                assert result == failed
                self.workflow = WorkflowStatus.FAILED
                event("a stalled workflow was failed")

    # two workers at the same moment
    @contextmanager
    def meanwhile(self, other_worker):
        """Run ``other_worker`` between the next transition's reads and its EXEC."""
        original = self.store.with_task_status
        fired = []

        def interleave(*args, **kwargs):
            if not fired:
                fired.append(True)
                other_worker()
            return original(*args, **kwargs)

        with patch.object(self.store, "with_task_status", side_effect=interleave):
            yield
        assert fired, "the other worker never ran"

    @precondition(lambda self: len(self.having(RUNNING)) >= 2)
    @rule(data=st.data(), first=outcomes, second=outcomes, produce=st.booleans())
    def two_tasks_finish_at_once(self, data, first, second, produce):
        """One worker's completion or failure lands inside another's."""
        pairs = list(permutations(self.having(RUNNING), 2))
        feeding_one_task = [
            (a, b)
            for a, b in pairs
            if any({a, b} <= task.predecessors for task in self.tasks.values())
        ]
        mine, theirs = data.draw(st.sampled_from(feeding_one_task or pairs))

        with self.meanwhile(lambda: self._finish(self.other, theirs, second, produce)):
            self._finish(self.engine, mine, first, produce)
        event("two tasks finished at once")

    @precondition(lambda self: self.queued())
    @rule()
    def two_workers_claim_at_once(self):
        """Both see the same head of the queue; each must get a different task."""
        with self.meanwhile(lambda: self._claimed(self.other.claim_task(WF))):
            self._claimed(self.engine.claim_task(WF))
        event("two workers claimed at once")

    # properties
    @invariant()
    def redis_matches_the_model_and_is_consistent(self):
        if self.workflow is None:  # not submitted yet
            return
        queue = self.redis.lrange(self.queue.queue_key(WF), 0, -1)
        delayed = set(self.redis.zrange(self.queue.delayed_key(WF), 0, -1))
        leased = set(self.redis.zrange(self.queue.processing_key(WF), 0, -1))
        attempts = self.redis.hgetall(self.queue.attempts_key(WF))
        blocked_since = self.redis.hgetall(self.queue.blocked_since_key(WF))
        states = {t: self.store.get_task(WF, t) for t in self.tasks}

        assert set(queue) | delayed | leased <= set(self.tasks), "unknown task scheduled"

        for task_id, task in self.tasks.items():
            state = states[task_id]
            where = f"{task_id} ({state.status.value})"
            assert state.status == task.status, f"{where}: the model expects {task.status.value}"

            # Its dependencies are the predecessors that have not completed.
            waiting_for = self.redis.smembers(f"deps:{WF}:{task_id}")
            assert waiting_for == {
                p for p in task.predecessors if states[p].status != COMPLETED
            }, where

            place = (queue.count(task_id), task_id in delayed, task_id in leased)
            expected = {
                PENDING: (0 if waiting_for else 1, False, False),
                RUNNING: (0, False, True),
                BLOCKED: (0, True, False),
                COMPLETED: (0, False, False),
                FAILED: (0, False, False),
            }[state.status]
            assert place == expected, f"{where}: queued/parked/leased is {place}"

            assert int(attempts.get(task_id, 0)) == task.attempts, where
            assert (task_id in blocked_since) == (task.attempts > 0), where

            assert (state.error is not None) == (state.status in (BLOCKED, FAILED)), where
            history = (state.claims, state.reclaims, state.deferrals, state.retries)
            modelled = (task.claims, task.reclaims, task.deferrals, task.retries)
            assert history == modelled, f"{where}: history {history}, the model expects {modelled}"
            if state.status == COMPLETED:
                assert len(state.output_refs) == int(task.produced), where

        workflow = self.store.get_workflow(WF)
        assert workflow.status == self.workflow
        running = workflow.status == WorkflowStatus.RUNNING
        assert (WF in self.redis.smembers(RUNNING_WORKFLOWS_KEY)) == running
        completed = workflow.status == WorkflowStatus.COMPLETED
        assert completed == all(s.status == COMPLETED for s in states.values())
        # The services key goes when no task will call a service again.
        assert bool(self.redis.exists(services_key_name(WF))) == (not completed)
        failed = workflow.status == WorkflowStatus.FAILED
        assert (workflow.error is not None) == failed
        if failed:
            assert any(s.status == FAILED for s in states.values())

    @invariant()
    def calls_with_nothing_to_do_change_nothing(self):
        """Claiming from an empty queue, polling with nothing due, recovering a
        service no task is blocked on, retrying or resuming what did not fail:
        each answers that there is nothing to do and leaves Redis as it was.
        Checked once per state the model reaches.
        """
        if self.workflow is None:  # not submitted yet
            return
        state = (self.workflow, *((t.status, t.due, t.lease_expired) for t in self.tasks.values()))
        if state == self.checked_idle:
            return
        self.checked_idle = state
        before = self.snapshot()
        if not self.queued():
            assert self.engine.claim_task(WF) is None
        if not self.due():
            assert self.engine.release_due_tasks(WF) == 0
        resumable = BLOCKED if self.workflow == WorkflowStatus.RUNNING else None
        for task_id, task in self.tasks.items():
            if task.status != resumable:
                assert self.engine.resume_blocked_tasks(WF, task.service) == 0
        for task_id in self.having(PENDING, RUNNING, BLOCKED, COMPLETED):
            with pytest.raises(NotFailedError):
                self.engine.retry_task(WF, task_id)
        if self.workflow != WorkflowStatus.FAILED:
            with pytest.raises(NotFailedError):
                self.engine.resume_workflow(WF)
        assert self.snapshot() == before

    @invariant()
    def the_runtime_verifier_finds_nothing_wrong(self):
        if self.workflow is None:
            return
        for workflow_id in (WF, BYSTANDER):
            problems = [
                f"{r.name}: {p}" for r in self.verifier.verify(workflow_id) for p in r.problems
            ]
            assert not problems, f"{workflow_id}: {problems}"

    @invariant()
    def a_running_workflow_has_something_to_wait_for(self):
        """Running means that a task is queued, parked or leased.
        """
        if self.workflow != WorkflowStatus.RUNNING:
            return
        assert self.queued() or self.having(RUNNING, BLOCKED, FAILED)

    # every reachable state can still be completed
    def teardown(self):
        if self.workflow is None:
            return
        event(f"sequence ended with the workflow {self.workflow.value}")
        for _ in range(len(self.tasks) + 2):
            if self.workflow == WorkflowStatus.COMPLETED:
                break
            if self.workflow == WorkflowStatus.FAILED:
                self.engine.resume_workflow(WF)
                for task_id in self.having(FAILED):
                    self._retried(task_id)
            # A running workflow can still hold failed tasks, see above.
            for task_id in self.having(FAILED):
                self.engine.retry_task(WF, task_id)
                self._retried(task_id)
            for task_id in self.having(BLOCKED):
                self.engine.resume_blocked_tasks(WF, self.tasks[task_id].service)
                self._requeued(task_id)
            while self.queued():
                self._claimed(self.engine.claim_task(WF))
            for task_id in self.having(RUNNING):
                self._finish(self.engine, task_id, "complete", True)
            self.redis_matches_the_model_and_is_consistent()

        assert self.workflow == WorkflowStatus.COMPLETED, "the workflow could not be completed"
        self.redis_matches_the_model_and_is_consistent()
        untouched = self.snapshot(BYSTANDER, index=False) == self.bystander
        assert untouched, "the other workflow changed"
        for key in (
            self.queue.queue_key(WF),
            self.queue.delayed_key(WF),
            self.queue.processing_key(WF),
            self.queue.attempts_key(WF),
            self.queue.blocked_since_key(WF),
        ):
            assert not self.redis.exists(key), f"{key} left behind"


SETTINGS = settings(
    max_examples=int(os.environ.get("ENGINE_FUZZ_EXAMPLES", "50")),
    stateful_step_count=50,
    deadline=None,
    print_blob=True,
)

EngineMachine.TestCase.settings = SETTINGS
TestEngineStateMachine = EngineMachine.TestCase
