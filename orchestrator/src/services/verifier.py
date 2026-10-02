"""Verification of a workflow's state in Redis, at runtime.

The engine promises a set of properties after every transition (see
docs/PROPERTY_TESTING.md, where the same properties are checked on generated
step sequences). This module checks them on a real workflow, on demand
(``GET /workflows/{id}/verify``) and when a workflow completes. It catches
what no single transition can: state damaged afterwards, for example writes
lost when Redis crashed inside its one-second fsync window, or a key edited by
hand.

All keys of the workflow are read in one ``MULTI``/``EXEC`` block, so the
checks see one consistent moment even while workers are writing.

Checks (docs/VERIFICATION.md):

* ``status``        the workflow's status agrees with its tasks and the running index
* ``placement``     every task is in exactly the one place its status implies
* ``dependencies``  each task waits exactly for its predecessors that have not completed
* ``handover``      each completed predecessor's outputs were handed to its dependents unchanged
* ``inputs``        each claimed task received exactly what was handed to it
* ``bookkeeping``   retry counts exist only for tasks that can still be deferred
* ``history``       each task's counters add up (claims cover deferrals and reclaims)
* ``cleanup``       a completed workflow left no scheduling state and no services key
"""

import json
from dataclasses import dataclass, field

from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus
from services.secrets import services_key_name
from services.state_store import RUNNING_WORKFLOWS_KEY, WorkflowNotFoundError
from services.workflow_engine import INITIAL_INPUTS, SOURCE_NODE_KEY, WorkflowEngine

CHECKS = (
    "status", "placement", "dependencies", "handover", "inputs", "bookkeeping",
    "history", "cleanup",
)


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


@dataclass
class Snapshot:
    """Everything the checks need, read at one moment."""

    workflow: WorkflowState
    tasks: dict[str, TaskState]  # task id -> state
    predecessors: dict[str, set[str]]  # task id -> its predecessors' task ids
    waiting_for: dict[str, set[str]]  # task id -> its dependency set
    handed: dict[str, dict[str, list[dict]]]  # task id -> producer -> refs handed to it
    queue: list[str]
    parked: set[str]
    leased: set[str]
    attempts: set[str]
    blocked_since: set[str]
    in_running_index: bool
    has_services_key: bool
    leftovers: list[str] = field(default_factory=list)  # scheduling keys that exist


@dataclass
class CheckResult:
    name: str
    problems: list[str]

    @property
    def passed(self) -> bool:
        return not self.problems


class WorkflowVerifier:
    """Reads a workflow's state and checks the engine's properties on it."""

    def __init__(self, engine: WorkflowEngine):
        if engine is None:
            raise ValueError("engine is required")
        self._engine = engine
        self._store = engine.state_store
        self._queue = engine.task_queue
        self._redis = engine.redis

    # ------------------------------------------------------------------
    # reading
    # ------------------------------------------------------------------
    def snapshot(self, workflow_id: str) -> Snapshot:
        """Read every key of the workflow in one transaction.

        Raises ``WorkflowNotFoundError`` for an unknown workflow.
        """
        store, queue, engine = self._store, self._queue, self._engine
        task_ids = sorted(store.task_ids(workflow_id))
        scheduling = [
            queue.queue_key(workflow_id), queue.delayed_key(workflow_id),
            queue.processing_key(workflow_id), queue.attempts_key(workflow_id),
            queue.blocked_since_key(workflow_id),
        ]

        pipe = self._redis.pipeline(transaction=True)
        pipe.get(store.workflow_key(workflow_id))
        for t in task_ids:
            pipe.get(store.task_key(workflow_id, t))
            pipe.smembers(engine.dependents_key(workflow_id, t))
            pipe.smembers(engine.deps_key(workflow_id, t))
            pipe.hgetall(queue.inputs_key(workflow_id, t))
        pipe.lrange(scheduling[0], 0, -1)
        pipe.zrange(scheduling[1], 0, -1)
        pipe.zrange(scheduling[2], 0, -1)
        pipe.hkeys(scheduling[3])
        pipe.hkeys(scheduling[4])
        pipe.sismember(RUNNING_WORKFLOWS_KEY, workflow_id)
        pipe.exists(services_key_name(workflow_id))
        for key in scheduling:
            pipe.exists(key)
        values = iter(pipe.execute())

        document = next(values)
        if document is None:
            raise WorkflowNotFoundError(workflow_id)
        tasks, dependents, waiting_for, handed = {}, {}, {}, {}
        for t in task_ids:
            tasks[t] = TaskState.model_validate_json(next(values))
            dependents[t] = {_decode(d) for d in next(values)}
            waiting_for[t] = {_decode(d) for d in next(values)}
            handed[t] = {
                _decode(producer): json.loads(_decode(payload))
                for producer, payload in next(values).items()
            }
        predecessors = {t: {p for p in task_ids if t in dependents[p]} for t in task_ids}
        snap = Snapshot(
            workflow=WorkflowState.model_validate_json(document),
            tasks=tasks,
            predecessors=predecessors,
            waiting_for=waiting_for,
            handed=handed,
            queue=[_decode(t) for t in next(values)],
            parked={_decode(t) for t in next(values)},
            leased={_decode(t) for t in next(values)},
            attempts={_decode(t) for t in next(values)},
            blocked_since={_decode(t) for t in next(values)},
            in_running_index=bool(next(values)),
            has_services_key=bool(next(values)),
        )
        snap.leftovers = [key for key in scheduling if next(values)]
        return snap

    def verify(self, workflow_id: str) -> list[CheckResult]:
        """Check one workflow; raises ``WorkflowNotFoundError`` if unknown."""
        return check(self.snapshot(workflow_id))


# ----------------------------------------------------------------------
# the checks: pure functions of a snapshot
# ----------------------------------------------------------------------
def check(snap: Snapshot) -> list[CheckResult]:
    """Every check, in the order of CHECKS, with the problems each found."""
    return [
        CheckResult(name, list(fn(snap)))
        for name, fn in (
            ("status", _status), ("placement", _placement), ("dependencies", _dependencies),
            ("handover", _handover), ("inputs", _inputs), ("bookkeeping", _bookkeeping),
            ("history", _history), ("cleanup", _cleanup),
        )
    ]


def _name(snap: Snapshot, task_id: str) -> str:
    task = snap.tasks.get(task_id)
    return f"{task.node_key} ({task_id})" if task else task_id


def _status(snap: Snapshot):
    status = snap.workflow.status
    statuses = [t.status for t in snap.tasks.values()]
    all_done = all(s == TaskStatus.COMPLETED for s in statuses)
    if (status == WorkflowStatus.COMPLETED) != all_done:
        yield (f"workflow is {status.value} but "
               f"{'all' if all_done else 'not all'} tasks are completed")
    if status == WorkflowStatus.FAILED:
        if TaskStatus.FAILED not in statuses:
            yield "workflow is failed but no task failed"
        if not snap.workflow.error:
            yield "workflow is failed but has no error"
    elif snap.workflow.error:
        yield f"workflow is {status.value} but still has an error"
    if snap.in_running_index != (status == WorkflowStatus.RUNNING):
        yield (f"workflow is {status.value} but is "
               f"{'' if snap.in_running_index else 'not '}in the running index")


def _placement(snap: Snapshot):
    for unknown in sorted((set(snap.queue) | snap.parked | snap.leased) - set(snap.tasks)):
        yield f"unknown task {unknown} is scheduled"
    for t, task in snap.tasks.items():
        place = (snap.queue.count(t), t in snap.parked, t in snap.leased)
        expected = {
            TaskStatus.PENDING: (0 if snap.waiting_for[t] else 1, False, False),
            TaskStatus.RUNNING: (0, False, True),
            TaskStatus.BLOCKED: (0, True, False),
            TaskStatus.COMPLETED: (0, False, False),
            TaskStatus.FAILED: (0, False, False),
        }[task.status]
        if place != expected:
            queued, parked, leased = place
            yield (f"{_name(snap, t)} is {task.status.value} but is queued {queued} "
                   f"time(s), {'' if parked else 'not '}parked and "
                   f"{'' if leased else 'not '}leased")


def _dependencies(snap: Snapshot):
    for t in snap.tasks:
        expected = {p for p in snap.predecessors[t] if snap.tasks[p].status != TaskStatus.COMPLETED}
        if snap.waiting_for[t] != expected:
            yield (f"{_name(snap, t)} waits for {sorted(snap.waiting_for[t])}, "
                   f"expected {sorted(expected)}")


def _ref_key(ref: dict) -> tuple:
    """What identifies the data a reference points to."""
    return (ref.get("protocol"), ref.get("uri"), ref.get("format"),
            ref.get("checksum"), ref.get("size_bytes"))


def _handover(snap: Snapshot):
    for t in snap.tasks:
        for p in snap.predecessors[t]:
            producer = snap.tasks[p]
            if producer.status != TaskStatus.COMPLETED:
                continue
            produced = [_ref_key(r.model_dump(mode="json")) for r in producer.output_refs]
            given = snap.handed[t].get(p)
            if not produced:
                if given:
                    yield f"{_name(snap, t)} was handed outputs {_name(snap, p)} never produced"
                continue
            if given is None:
                yield f"{_name(snap, p)} completed but its outputs never reached {_name(snap, t)}"
                continue
            if [_ref_key(r) for r in given] != produced:
                yield (f"{_name(snap, t)} was handed outputs of {_name(snap, p)} "
                       f"that differ from what it produced (uri, checksum or size)")
            if any(r.get("metadata", {}).get(SOURCE_NODE_KEY) != producer.node_key for r in given):
                yield f"outputs of {_name(snap, p)} handed to {_name(snap, t)} name the wrong producer"
        for producer in set(snap.handed[t]) - snap.predecessors[t] - {INITIAL_INPUTS}:
            yield f"{_name(snap, t)} holds inputs from {producer}, which is not a predecessor"


def _inputs(snap: Snapshot):
    for t, task in snap.tasks.items():
        if task.claims == 0 or task.status == TaskStatus.PENDING and snap.waiting_for[t]:
            continue
        stored = sorted(
            _ref_key(r) + (r.get("metadata", {}).get(SOURCE_NODE_KEY),)
            for refs in snap.handed[t].values() for r in refs
        )
        received = sorted(
            _ref_key(r.model_dump(mode="json")) + (r.metadata.get(SOURCE_NODE_KEY),)
            for r in task.input_refs
        )
        if received != stored:
            yield (f"{_name(snap, t)} received {len(received)} input(s), "
                   f"but {len(stored)} were handed to it")


def _bookkeeping(snap: Snapshot):
    final = (TaskStatus.COMPLETED, TaskStatus.FAILED)
    for t, task in snap.tasks.items():
        if task.status in final and (t in snap.attempts or t in snap.blocked_since):
            yield f"{_name(snap, t)} is {task.status.value} but still has retry bookkeeping"


def _history(snap: Snapshot):
    """Every claim ends in a deferral, a reclaim, a failure or the completion,
    or is still running; failures from a running task are at most the retries
    (plus the current failure). So, per task:

        0 <= claims - deferrals - reclaims - [running or completed] <= retries + [failed]
    """
    for t, task in snap.tasks.items():
        open_claim = int(task.status in (TaskStatus.RUNNING, TaskStatus.COMPLETED))
        ended_in_failure = task.claims - task.deferrals - task.reclaims - open_claim
        limit = task.retries + int(task.status == TaskStatus.FAILED)
        if not 0 <= ended_in_failure <= limit:
            yield (f"{_name(snap, t)}: {task.claims} claims do not add up with "
                   f"{task.deferrals} deferrals, {task.reclaims} reclaims and "
                   f"{task.retries} retries while {task.status.value}")


def _cleanup(snap: Snapshot):
    if snap.workflow.status != WorkflowStatus.COMPLETED:
        return
    for key in snap.leftovers:
        yield f"completed, but {key} still exists"
    if snap.has_services_key:
        yield "completed, but its services key is still stored"
