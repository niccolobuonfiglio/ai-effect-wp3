"""Shared helpers for the system tests.
"""

import json
import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
ORCHESTRATOR_COMPOSE = REPO_ROOT / "orchestrator" / "docker-compose.yml"
USE_CASE = REPO_ROOT / "use-cases" / "file_based_energy_pipeline"
USE_CASE_COMPOSE = USE_CASE / "docker-compose.yml"

API = os.environ.get("ORCHESTRATOR_URL", "http://localhost:18000").rstrip("/")
API_KEY = os.environ.get("ORCHESTRATOR_API_KEY") or None
SERVICE_API_KEY = os.environ.get("SERVICE_API_KEY") or None
REDIS_URL = os.environ.get("SYSTEM_TEST_REDIS_URL", "redis://localhost:16379/0")

SERVICE = "data_analyzer1"
COMPOSE_SERVICE = "data-analyzer"
SERVICE_HEALTH_URL = "http://localhost:18083/health"

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
    body = {"blueprint": blueprint, "dockerinfo": dockerinfo}
    if SERVICE_API_KEY:
        body["services_api_key"] = SERVICE_API_KEY
    return body


def submit() -> str:
    response = api("POST", "/workflows", json=payload())
    assert response.status_code == 200, response.text
    return response.json()["workflow_id"]


def workflow(wf: str) -> dict:
    return api("GET", f"/workflows/{wf}").json()


def tasks(wf: str) -> dict[str, dict]:
    body = api("GET", f"/workflows/{wf}/tasks").json()
    return {t["node_key"]: t for t in body["tasks"]}


def verification(wf: str) -> dict:
    """The orchestrator's own check of a workflow's state (docs/VERIFICATION.md)."""
    return api("GET", f"/workflows/{wf}/verify").json()


def problems(report: dict) -> list[str]:
    return [f"{c['name']}: {p}" for c in report["checks"] for p in c["problems"]]


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


def require_live_stack() -> None:
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

    for name, url in PIPELINE.items():
        wait_for(lambda url=url: answers(url), timeout=60, what=f"{name} to answer")

    for name in PIPELINE:
        api("POST", f"/orchestrator/health/services/{name}/recover")
