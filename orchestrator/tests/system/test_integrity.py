"""Data integrity across the pipeline, against the live stacks.
"""

import pytest

from tests.system.live import (
    COMPOSE_SERVICE,
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
)


@pytest.fixture(scope="module", autouse=True)
def live_stack():
    require_live_stack()


def test_every_output_carries_checksum_and_size():
    wf = submit()
    assert wait_for(lambda: finished(wf), timeout=60, what=f"{wf} to finish") == "completed"

    for node_key in (
        "data_generator1:GenerateData",
        "data_analyzer1:AnalyzeData",
        "report_generator1:GenerateReport",
    ):
        (output,) = tasks(wf)[node_key]["output_refs"]
        assert output["checksum"].startswith("sha256:"), node_key
        assert output["size_bytes"] > 0, node_key

    produced = tasks(wf)["data_generator1:GenerateData"]["output_refs"][0]
    (received,) = tasks(wf)["data_analyzer1:AnalyzeData"]["input_refs"]
    assert received["checksum"] == produced["checksum"]
    assert received["size_bytes"] == produced["size_bytes"]


def test_file_changed_between_tasks_is_refused():
    blocked = f"{SERVICE}:AnalyzeData"
    compose(USE_CASE_COMPOSE, "stop", COMPOSE_SERVICE)
    try:
        wf = submit()
        wait_for(
            lambda: tasks(wf)[blocked]["status"] == "blocked",
            timeout=60,
            what=f"{blocked} to be blocked",
        )
        generated = tasks(wf)["data_generator1:GenerateData"]["output_refs"][0]["uri"]
        compose(
            USE_CASE_COMPOSE, "exec", "-T", "data-generator",
            "sed", "-i", "2s/120.5/999.9/", generated,
        )
    finally:
        start_service()

    api("POST", f"/orchestrator/health/services/{SERVICE}/recover")
    assert wait_for(lambda: finished(wf), timeout=90, what=f"{wf} to finish") == "failed"

    analyzer = tasks(wf)[blocked]
    assert analyzer["status"] == "failed"
    assert "Input rejected" in analyzer["error"]
    assert "checksum mismatch" in analyzer["error"]
    assert wf in analyzer["error"]  # the workflow's own file, not a shared one
    # Nothing downstream ran on the altered data
    assert tasks(wf)["report_generator1:GenerateReport"]["status"] == "pending"
