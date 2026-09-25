"""Automated version of the manual outage/recovery runs (run 3 and run 4).

Runs against the live stacks: the orchestrator from ``orchestrator/`` and the
``file_based_energy_pipeline`` use case. One service (``data-analyzer``) is
stopped to simulate an outage, and the test checks that

* the outage is detected, alerted and shown by the health API,
* the task is BLOCKED while the workflow keeps running,
* one recover call resumes the *same* workflow, which then completes,
* already completed tasks are not executed again.

The stopped service is always restarted, even when an assertion fails.

Addresses, compose files and the API wrappers come from ``live.py``, which the
other system modules share.
"""

import time
from datetime import datetime, timezone

import httpx
import pytest

from tests.system.live import (
    API,
    COMPOSE_SERVICE,
    EXPECTED_TASKS,
    ORCHESTRATOR_COMPOSE,
    SERVICE,
    USE_CASE_COMPOSE,
    api,
    compose,
    finished,
    require_live_stack,
    service_health,
    start_service,
    submit,
    tasks,
    wait_for,
    workflow,
)

# ----------------------------------------------------------------------
# fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def live_stack():
    """Fail fast, with a clear message, if the stacks are not up."""
    require_live_stack()


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

    # The same outage as the dashboard sees it.
    listed = api("GET", "/workflows").json()["workflows"]
    assert any(w["workflow_id"] == wf and w["status"] == "running" for w in listed)

    graph = {n["key"]: n for n in api("GET", f"/workflows/{wf}/graph").json()["nodes"]}
    assert graph[f"{SERVICE}:AnalyzeData"]["status"] == "blocked"
    assert graph[f"{SERVICE}:AnalyzeData"]["service_status"] == "UNHEALTHY"
    assert graph["input_provider1:GetConfiguration"]["status"] == "completed"

    # Alerts from all worker replicas are read from the shared logs volume.
    alerts = api("GET", "/orchestrator/alerts?limit=100").json()["alerts"]
    assert any(a["alert"] == "SERVICE_DOWN" and a["service_name"] == SERVICE for a in alerts)

    assert wf in api("GET", f"/workflows/{wf}/logs").text

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


def test_dashboard_page_is_served():
    response = httpx.get(f"{API}/ui/", timeout=5)

    assert response.status_code == 200
    assert "Orchestrator Dashboard" in response.text


def test_service_is_healthy_again_for_new_workflows():
    """After recovery the service is used normally, with no leftover state."""
    wf = submit()

    assert wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish") == "completed"
    assert service_health(SERVICE)["failure_count"] == 0
