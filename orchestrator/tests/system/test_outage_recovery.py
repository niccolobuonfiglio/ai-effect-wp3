"""Automated version of the manual outage/recovery runs (run 3 and run 4).

Runs against the live stacks: the orchestrator from ``orchestrator/`` and the
``file_based_energy_pipeline`` use case. One service (``data-analyzer``) is
stopped to simulate an outage, and the test checks that

* the outage is detected, alerted and shown by the health API,
* the task is BLOCKED while the workflow keeps running,
* one recover call resumes the *same* workflow, which then completes,
* already completed tasks are not executed again.

The stopped service is always restarted, even when an assertion fails.

Settings (environment):
    ORCHESTRATOR_URL      default http://localhost:18000
    ORCHESTRATOR_API_KEY  sent as bearer token when set
"""

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
ORCHESTRATOR_COMPOSE = REPO_ROOT / "orchestrator" / "docker-compose.yml"
USE_CASE = REPO_ROOT / "use-cases" / "file_based_energy_pipeline"
USE_CASE_COMPOSE = USE_CASE / "docker-compose.yml"

API = os.environ.get("ORCHESTRATOR_URL", "http://localhost:18000").rstrip("/")
API_KEY = os.environ.get("ORCHESTRATOR_API_KEY") or None

# The service taken down, as the orchestrator and as Docker Compose name it.
SERVICE = "data_analyzer1"
COMPOSE_SERVICE = "data-analyzer"
SERVICE_HEALTH_URL = "http://localhost:18083/health"

# Every service of the pipeline: orchestrator name -> host health URL.
PIPELINE = {
    "input_provider1": "http://localhost:18081/health",
    "data_generator1": "http://localhost:18082/health",
    "data_analyzer1": "http://localhost:18083/health",
    "report_generator1": "http://localhost:18084/health",
}

EXPECTED_TASKS = {
    "input_provider1:GetConfiguration",
    "data_generator1:GenerateData",
    "data_analyzer1:AnalyzeData",
    "report_generator1:GenerateReport",
}


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def api(method: str, path: str, **kwargs) -> httpx.Response:
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    return httpx.request(method, f"{API}{path}", headers=headers, timeout=10, **kwargs)


def compose(compose_file: Path, *args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def wait_for(condition, timeout: float, interval: float = 1.0, what: str = "condition"):
    """Poll until ``condition()`` returns a truthy value, then return it."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = condition()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}; last value: {last!r}")


def payload() -> dict:
    blueprint = json.loads((USE_CASE / "export" / "blueprint.json").read_text())
    dockerinfo = json.loads((USE_CASE / "export" / "dockerinfo.json").read_text())
    return {"blueprint": blueprint, "dockerinfo": dockerinfo}


def submit() -> str:
    response = api("POST", "/workflows", json=payload())
    assert response.status_code == 200, response.text
    return response.json()["workflow_id"]


def workflow(wf: str) -> dict:
    return api("GET", f"/workflows/{wf}").json()


def tasks(wf: str) -> dict[str, dict]:
    body = api("GET", f"/workflows/{wf}/tasks").json()
    return {t["node_key"]: t for t in body["tasks"]}


def service_health(name: str) -> dict:
    return api("GET", f"/orchestrator/health/services/{name}").json()


def finished(wf: str):
    status = workflow(wf)["status"]
    return status if status in ("completed", "failed") else None


def answers(url: str) -> bool:
    try:
        return httpx.get(url, timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


def start_service() -> None:
    compose(USE_CASE_COMPOSE, "start", COMPOSE_SERVICE)
    wait_for(lambda: answers(SERVICE_HEALTH_URL), timeout=30, what=f"{COMPOSE_SERVICE} to answer")


# ----------------------------------------------------------------------
# fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def live_stack():
    """Fail fast, with a clear message, if the stacks are not up."""
    try:
        health = httpx.get(f"{API}/health", timeout=5)
    except httpx.HTTPError as e:
        pytest.fail(f"orchestrator not reachable at {API}: {e}")
    assert health.status_code == 200, f"{API}/health returned {health.status_code}"

    probe = api("GET", "/orchestrator/health/services")
    if probe.status_code == 404:
        pytest.fail("the running orchestrator has no health API: it is the old code, rebuild it")
    if probe.status_code == 401:
        pytest.fail("the orchestrator requires a key: export ORCHESTRATOR_API_KEY")

    running = compose(USE_CASE_COMPOSE, "ps", "--status", "running", "--services").split()
    missing = {"input-provider", "data-generator", "data-analyzer", "report-generator"} - set(running)
    assert not missing, f"use-case services not running: {sorted(missing)}"

    # A container can be "running" before its server accepts connections. A
    # workflow submitted in that window fails its first call, which opens the
    # circuit for up to a minute and slows every later test.
    for name, url in PIPELINE.items():
        wait_for(lambda url=url: answers(url), timeout=60, what=f"{name} to answer")

    # Start from a clean breaker: circuits opened by earlier runs live in Redis.
    for name in PIPELINE:
        api("POST", f"/orchestrator/health/services/{name}/recover")


@pytest.fixture
def outage():
    """Stop the service for the duration of a test, and always bring it back."""
    compose(USE_CASE_COMPOSE, "stop", COMPOSE_SERVICE)
    try:
        yield
    finally:
        start_service()


# ----------------------------------------------------------------------
# tests
# ----------------------------------------------------------------------
def test_baseline_workflow_completes():
    wf = submit()

    assert wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish") == "completed"
    result = tasks(wf)
    assert set(result) == EXPECTED_TASKS
    assert all(t["status"] == "completed" for t in result.values())


def test_outage_blocks_task_and_recovery_resumes_same_workflow(outage):
    since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    wf = submit()

    # --- during the outage -------------------------------------------------
    blocked = wait_for(
        lambda: tasks(wf)[f"{SERVICE}:AnalyzeData"]["status"] == "blocked",
        timeout=60,
        what=f"{SERVICE} task to be blocked",
    )
    assert blocked

    assert workflow(wf)["status"] == "running"
    during = tasks(wf)
    assert during["input_provider1:GetConfiguration"]["status"] == "completed"
    assert during["data_generator1:GenerateData"]["status"] == "completed"
    assert during["report_generator1:GenerateReport"]["status"] == "pending"
    assert "unavailable" in during[f"{SERVICE}:AnalyzeData"]["error"]
    completed_before = {
        key: during[key]["updated_at"]
        for key in ("input_provider1:GetConfiguration", "data_generator1:GenerateData")
    }

    # The task blocks on the first refused call; the circuit opens after
    # CIRCUIT_FAILURE_THRESHOLD (3) failures, i.e. a few seconds of backoff later.
    health = wait_for(
        lambda: (h := service_health(SERVICE))["status"] == "UNHEALTHY" and h,
        timeout=30,
        what=f"{SERVICE} to be reported UNHEALTHY",
    )
    assert health["circuit_state"] == "open"
    assert "Connection" in health["last_error"]

    summary = api("GET", "/orchestrator/health/services").json()
    assert summary["overall_status"] == "UNHEALTHY"

    logs = compose(ORCHESTRATOR_COMPOSE, "logs", "--since", since, "worker")
    assert f"SERVICE DOWN: {SERVICE}" in logs

    # --- recovery ----------------------------------------------------------
    start_service()
    response = api("POST", f"/orchestrator/health/services/{SERVICE}/recover")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "HEALTHY"
    # 0 is possible if a retry was in flight at that instant; the task then
    # runs on that retry or the next one, so the outcome below is the same.
    assert body["resumed_tasks"] in (0, 1)

    # --- after -------------------------------------------------------------
    outcome = wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish")
    assert outcome == "completed", workflow(wf)

    after = tasks(wf)
    assert all(t["status"] == "completed" for t in after.values())
    for key, stamp in completed_before.items():
        assert after[key]["updated_at"] == stamp, f"{key} was executed again"

    recovered = service_health(SERVICE)
    assert recovered["status"] == "HEALTHY"
    assert recovered["circuit_state"] == "closed"
    assert recovered["manually_recovered_at"] is not None


def test_service_back_within_seconds_costs_seconds_not_a_minute(outage):
    """A service that is only briefly unavailable - e.g. still starting after
    ``docker compose up`` - must not cost a full retry and reset cycle.

    Before backoff (fixed 30 s retry, 60 s reset, threshold 1) this took
    60-90 s. Now retries start at 2 s and the circuit only opens after three
    failures, so the workflow finishes seconds after the service is back,
    without anyone calling /recover.
    """
    wf = submit()
    wait_for(
        lambda: tasks(wf)[f"{SERVICE}:AnalyzeData"]["status"] == "blocked",
        timeout=30,
        what=f"{SERVICE} task to be blocked",
    )

    time.sleep(2)                 # the service stays down a little longer
    back_at = time.monotonic()
    start_service()               # no /recover call

    outcome = wait_for(lambda: finished(wf), timeout=30, what=f"{wf} to finish")
    elapsed = time.monotonic() - back_at

    print(f"\nworkflow finished {elapsed:.1f}s after the service came back")
    assert outcome == "completed", workflow(wf)
    assert elapsed < 20, f"took {elapsed:.1f}s after the service came back"
    assert service_health(SERVICE)["circuit_state"] == "closed"


def test_service_is_healthy_again_for_new_workflows():
    """After recovery the service is used normally, with no leftover state."""
    wf = submit()

    assert wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish") == "completed"
    assert service_health(SERVICE)["failure_count"] == 0
