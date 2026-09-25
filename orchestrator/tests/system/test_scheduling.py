"""Scheduling safety nets, against the live stacks.

Two things that cannot be shown with mocks alone:

* a task whose worker disappears mid-flight is picked up by another worker and
  the workflow still finishes (leases and reclaim),
* a workflow that failed can be resumed from where it stopped, without
  re-running what already succeeded.

To make both deterministic the orchestrator's workers are stopped for a moment
and the test process itself plays the part of the worker that dies: it claims a
task through the real engine against the live Redis and then simply stops,
exactly as a killed container would. The workers are always started again, even
when an assertion fails.

Settings (environment): see ``live.py``.
"""

import time

import pytest
import redis

from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine
from tests.system.live import (
    COMPOSE_SERVICE,
    EXPECTED_TASKS,
    ORCHESTRATOR_COMPOSE,
    REDIS_URL,
    SERVICE,
    USE_CASE_COMPOSE,
    api,
    compose,
    finished,
    require_live_stack,
    start_service,
    submit,
    tasks,
    wait_for,
    workflow,
)

# Short enough that a reclaim happens within a test, long enough that a claim
# does not expire before the test has looked at it.
LEASE = 5.0


@pytest.fixture(scope="module", autouse=True)
def live_stack():
    """Fail fast, with a clear message, if the stacks are not up."""
    require_live_stack()


@pytest.fixture
def engine():
    """An engine on the live Redis, standing in for a worker process."""
    client = redis.from_url(REDIS_URL, decode_responses=True)
    yield WorkflowEngine(
        RedisStateStore(client), RedisTaskQueue(client), client, lease_seconds=LEASE
    )
    client.close()


@pytest.fixture
def workers_stopped():
    """Stop the orchestrator's workers for a test, and always bring them back."""
    compose(ORCHESTRATOR_COMPOSE, "stop", "worker")
    try:
        yield
    finally:
        compose(ORCHESTRATOR_COMPOSE, "start", "worker")


def task_id_of(wf: str, node_key: str) -> str:
    return tasks(wf)[node_key]["task_id"]


def test_task_whose_worker_dies_is_rerun(workers_stopped, engine):
    """A claimed task with an expired lease is re-queued and runs to completion."""
    wf = submit()

    # Play the worker that dies: claim the first task (which takes a lease and
    # marks it running), then do nothing at all.
    claimed = wait_for(
        lambda: engine.claim_task(wf, timeout=1), timeout=30, what="a task to claim"
    )
    assert claimed.node_key == "input_provider1:GetConfiguration"
    assert tasks(wf)[claimed.node_key]["status"] == "running"

    # Nothing happens while the lease is held.
    assert finished(wf) is None

    compose(ORCHESTRATOR_COMPOSE, "start", "worker")
    time.sleep(LEASE)

    outcome = wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")
    assert outcome == "completed", workflow(wf)

    result = tasks(wf)
    assert set(result) == EXPECTED_TASKS
    assert all(t["status"] == "completed" for t in result.values())


def test_reclaim_is_logged(workers_stopped, engine):
    """The orchestrator says which task it took back, for the operator's benefit."""
    wf = submit()
    claimed = wait_for(
        lambda: engine.claim_task(wf, timeout=1), timeout=30, what="a task to claim"
    )

    compose(ORCHESTRATOR_COMPOSE, "start", "worker")
    wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")

    logs = compose(ORCHESTRATOR_COMPOSE, "logs", "--tail", "400", "worker")
    assert "lost its worker" in logs
    assert claimed.task_id in logs


def test_failed_workflow_resumes_where_it_stopped(workers_stopped, engine):
    """Resume re-runs only the failed task, keeping completed results."""
    # Let the pipeline run normally first, so real outputs exist.
    compose(ORCHESTRATOR_COMPOSE, "start", "worker")
    wf = submit()
    assert wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish") == "completed"

    # Now fail its last task, as a service that reports an error would.
    compose(ORCHESTRATOR_COMPOSE, "stop", "worker")
    last = "report_generator1:GenerateReport"
    engine.fail_task(wf, task_id_of(wf, last), "simulated failure for the resume test")

    before = tasks(wf)
    assert workflow(wf)["status"] == "failed"
    assert before[last]["status"] == "failed"
    completed_before = {
        key: task["updated_at"]
        for key, task in before.items()
        if task["status"] == "completed"
    }
    assert len(completed_before) == 3

    response = api("POST", f"/workflows/{wf}/resume")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "running"
    assert body["retried_tasks"] == [before[last]["task_id"]]

    compose(ORCHESTRATOR_COMPOSE, "start", "worker")
    assert wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish") == "completed"

    after = tasks(wf)
    assert all(t["status"] == "completed" for t in after.values())
    for key, stamp in completed_before.items():
        assert after[key]["updated_at"] == stamp, f"{key} was executed again"


def test_resume_refuses_a_workflow_that_did_not_fail():
    """A running or completed workflow is not resumable."""
    wf = submit()
    wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")

    response = api("POST", f"/workflows/{wf}/resume")

    assert response.status_code == 409
    assert "only failed workflows" in response.json()["detail"]


def test_retry_refuses_a_completed_task():
    """Completed work is never re-run by a retry call."""
    wf = submit()
    wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")
    task_id = task_id_of(wf, "data_analyzer1:AnalyzeData")

    response = api("POST", f"/workflows/{wf}/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "only failed tasks" in response.json()["detail"]
