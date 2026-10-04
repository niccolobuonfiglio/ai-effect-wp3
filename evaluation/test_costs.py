"""Checks of the Amendment 5 instruments.

Run with any pytest: ``python -m pytest evaluation/test_costs.py``. 
"""

import pytest

import runner
from runner import ACCESS_LINE, cgroup_value, parse_info


def test_access_lines_are_counted_by_method_path_and_status():
    logs = "\n".join([
        'INFO:     172.19.0.5:48120 - "POST /control/execute HTTP/1.1" 200 OK',
        'INFO:     172.19.0.5:48122 - "POST /control/execute HTTP/1.1" 422 Unprocessable Content',
        'INFO:     172.17.0.1:50000 - "GET /health HTTP/1.1" 200 OK',
        "INFO:     Execute: method=AnalyzeData, task=abc",  # not an access line
    ])
    found = [f"{m.group(1)} {m.group(2)} {m.group(3)}" for m in ACCESS_LINE.finditer(logs)]
    assert found == ["POST /control/execute 200", "POST /control/execute 422", "GET /health 200"]


def test_redis_info_is_parsed_without_section_headers():
    info = parse_info("# Stats\r\ntotal_commands_processed:1234\r\nused_memory:999\r\n\r\n# CPU\r\n")
    assert info == {"total_commands_processed": "1234", "used_memory": "999"}


def test_cgroup_counters_are_read_from_the_host(tmp_path, monkeypatch):
    scope = tmp_path / "docker-abc.scope"
    scope.mkdir()
    (scope / "cpu.stat").write_text("usage_usec 5000000\nuser_usec 3000000\nsystem_usec 2000000\n")
    (scope / "memory.current").write_text("1048576\n")
    monkeypatch.setattr(runner, "CGROUP_ROOT", tmp_path)
    assert cgroup_value("abc", "cpu.stat", "usage_usec") == 5_000_000
    assert cgroup_value("abc", "memory.current") == 1_048_576
    assert cgroup_value("missing", "memory.current") is None


def window(seconds, cpu, commands, requests, workflows=0):
    return {"seconds": seconds, "workflows": workflows, "redis_commands": commands,
            "cpu_seconds": {"orchestrator-api-1": cpu / 2, "orchestrator-worker-1": cpu / 2},
            "memory_bytes": {"orchestrator-api-1": 2**20, "orchestrator-worker-1": 2**20},
            "service_requests": requests}


def test_per_workflow_cost_subtracts_the_idle_rate():
    pytest.importorskip("numpy")
    from analyze import per_workflow_cost, window_rates

    trial = {"idle": window(60, 1.0, 600, {}),
             "trickle": window(120, 4.0, 2200, {"POST /control/execute 200": 40,
                                                "POST /control/execute 422": 40}, workflows=10)}
    cost = per_workflow_cost(trial)
    # idle rate over 120 s: 2 CPU s and 1200 commands; the rest belongs to 10 workflows
    assert cost["cpu_ms"] == pytest.approx(200)
    assert cost["redis_commands"] == pytest.approx(100)
    assert cost["service_requests"] == pytest.approx(8)
    rates = window_rates(trial["idle"])
    assert rates["cpu_s_per_min"] == pytest.approx(1.0)
    assert rates["cpu_s_per_min_by_role"] == {"api": pytest.approx(0.5), "worker": pytest.approx(0.5)}
    assert rates["memory_mib"] == pytest.approx(2)


def test_submission_slope():
    pytest.importorskip("scipy")
    from analyze import _slope

    fit = _slope([0, 10, 20, 0, 10, 20], [5, 25, 45, 5, 25, 45])
    assert fit["slope"] == pytest.approx(2)
    assert _slope([10, 10], [1, 2]) is None
