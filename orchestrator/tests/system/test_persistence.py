"""Redis persistence, against the live stacks.
"""

import json
import subprocess
import time

import httpx
import pytest
import redis

from services.state_store import RUNNING_WORKFLOWS_KEY, RedisStateStore
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
    service_health,
    start_service,
    submit,
    problems,
    tasks,
    verification,
    wait_for,
    workflow,
)

REDIS_SERVICE = "redis"
FSYNC_WINDOW = 2.0
LEASE = 5.0

BLOCKED = f"{SERVICE}:AnalyzeData"
COMPLETED_BEFORE_OUTAGE = ("input_provider1:GetConfiguration", "data_generator1:GenerateData")


# helpers
def docker(*args: str) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def redis_container() -> str:
    """Id of the Redis container, running or not."""
    return compose(ORCHESTRATOR_COMPOSE, "ps", "-a", "-q", REDIS_SERVICE).strip()


def redis_answers(client: redis.Redis) -> bool:
    try:
        return bool(client.ping())
    except redis.RedisError:  
        return False


def api_answers() -> bool:
    """Whether the API reaches Redis again (it reconnects on its own)."""
    try:
        return api("GET", "/workflows").status_code == 200
    except httpx.HTTPError:
        return False


def start_redis(client: redis.Redis) -> None:
    compose(ORCHESTRATOR_COMPOSE, "up", "-d", REDIS_SERVICE)
    wait_for(lambda: redis_answers(client), timeout=30, interval=0.2, what="Redis to answer")
    wait_for(api_answers, timeout=30, interval=0.5, what="the API to reach Redis again")


def kill_redis() -> str:
    """SIGKILL Redis: no clean shutdown, so no final snapshot. Returns its id."""
    container = redis_container()
    docker("kill", "-s", "KILL", container)
    wait_for(
        lambda: docker("inspect", "-f", "{{.State.Running}}", container) == "false",
        timeout=10,
        interval=0.2,
        what="Redis to be gone",
    )
    return container


def crash_redis(client: redis.Redis) -> None:
    """Kill Redis and start the same container again."""
    time.sleep(FSYNC_WINDOW)
    kill_redis()
    start_redis(client)


def running_services() -> list[str]:
    """Compose service of every running orchestrator container (one per replica)."""
    names = compose(
        ORCHESTRATOR_COMPOSE, "ps", "--status", "running", "--format", "{{.Service}}"
    )
    return sorted(names.split())


# fixtures
@pytest.fixture(scope="module", autouse=True)
def live_stack():
    """Fail fast, with a clear message, if the stacks are not up."""
    require_live_stack()


@pytest.fixture
def client():
    """A client on the live Redis; Redis is running again when the test ends."""
    client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield client
    finally:
        start_redis(client)
        client.close()


@pytest.fixture
def engine(client):
    """An engine on the live Redis, standing in for a worker process."""
    return WorkflowEngine(
        RedisStateStore(client), RedisTaskQueue(client), client, lease_seconds=LEASE
    )


@pytest.fixture
def outage():
    """Stop the service for the duration of a test, and always bring it back."""
    compose(USE_CASE_COMPOSE, "stop", COMPOSE_SERVICE)
    try:
        yield
    finally:
        start_service()


@pytest.fixture
def workers_stopped():
    """Stop the orchestrator's workers for a test, and always bring them back."""
    compose(ORCHESTRATOR_COMPOSE, "stop", "worker")
    try:
        yield
    finally:
        compose(ORCHESTRATOR_COMPOSE, "start", "worker")


# tests
def test_redis_runs_with_append_only_file_on_a_named_volume(client):
    """The configuration every other test here depends on (PERSISTENCE.md section 2)."""
    assert client.config_get("appendonly") == {"appendonly": "yes"}
    assert client.config_get("appendfsync") == {"appendfsync": "everysec"}

    persistence = client.info("persistence")
    assert persistence["aof_enabled"] == 1
    assert persistence["aof_last_write_status"] == "ok"

    (data,) = [
        m
        for m in json.loads(docker("inspect", "-f", "{{json .Mounts}}", redis_container()))
        if m["Destination"] == "/data"
    ]
    assert data["Type"] == "volume"
    assert data["Name"].endswith("redis_data"), data["Name"]


def test_workflow_interrupted_by_a_redis_crash_completes(client, outage):
    """Redis dies under a half-done workflow; the same workflow then completes.
    """
    wf = submit()
    wait_for(
        lambda: tasks(wf)[BLOCKED]["status"] == "blocked",
        timeout=60,
        what=f"{BLOCKED} to be blocked",
    )
    wait_for(
        lambda: service_health(SERVICE)["status"] == "UNHEALTHY",
        timeout=30,
        what=f"{SERVICE} to be reported UNHEALTHY",
    )
    before = tasks(wf)
    completed_before = {key: before[key] for key in COMPLETED_BEFORE_OUTAGE}
    assert all(t["status"] == "completed" for t in completed_before.values())
    services_before = running_services()

    crash_redis(client)

    assert running_services() == services_before
    assert workflow(wf).get("status") == "running", f"{wf} after the crash: {workflow(wf)}"
    assert wf in client.smembers(RUNNING_WORKFLOWS_KEY)

    restored = tasks(wf)
    assert set(restored) == EXPECTED_TASKS
    for key, task in completed_before.items():
        assert restored[key] == task, f"{key} changed across the crash"
    assert restored["report_generator1:GenerateReport"]["status"] == "pending"
    wait_for(
        lambda: tasks(wf)[BLOCKED]["status"] == "blocked",
        timeout=60,
        what=f"{BLOCKED} to be blocked again",
    )
    assert service_health(SERVICE)["status"] == "UNHEALTHY"
    report = verification(wf)
    assert report["verified"], problems(report)

    # --- the workflow goes on 
    start_service()
    response = api("POST", f"/orchestrator/health/services/{SERVICE}/recover")
    assert response.status_code == 200, response.text

    outcome = wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")
    assert outcome == "completed", workflow(wf)

    after = tasks(wf)
    assert all(t["status"] == "completed" for t in after.values())
    for key, task in completed_before.items():
        assert after[key]["updated_at"] == task["updated_at"], f"{key} was executed again"


def test_task_leased_during_a_redis_crash_is_reclaimed(client, workers_stopped, engine):
    """A task in flight when Redis dies keeps its lease and is re-run.
    """
    wf = submit()
    claimed = wait_for(
        lambda: engine.claim_task(wf, timeout=1), timeout=30, what="a task to claim"
    )
    queue = RedisTaskQueue(client)
    assert queue.leased_tasks(wf) == [claimed.task_id]

    crash_redis(client)

    assert queue.leased_tasks(wf) == [claimed.task_id], "the lease was lost with Redis"
    assert tasks(wf)[claimed.node_key]["status"] == "running"
    assert workflow(wf)["status"] == "running"

    compose(ORCHESTRATOR_COMPOSE, "start", "worker")
    outcome = wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish")
    assert outcome == "completed", workflow(wf)

    result = tasks(wf)
    assert set(result) == EXPECTED_TASKS
    assert all(t["status"] == "completed" for t in result.values())
    assert queue.leased_tasks(wf) == []


def test_finished_workflows_survive_redis_being_recreated(client):
    """Kill Redis, remove its container, create a new one: the history stays.
    """
    wf = submit()
    assert wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish") == "completed"
    workflow_before, tasks_before = workflow(wf), tasks(wf)
    keys_before = client.dbsize()

    time.sleep(FSYNC_WINDOW)
    old_container = kill_redis()
    docker("rm", old_container)
    start_redis(client)

    assert redis_container() != old_container
    assert workflow(wf) == workflow_before
    assert tasks(wf) == tasks_before
    assert client.dbsize() >= keys_before

    again = submit()
    assert wait_for(lambda: finished(again), timeout=60, what=f"{again} to finish") == "completed"
