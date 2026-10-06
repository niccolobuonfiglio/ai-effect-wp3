"""
Run with any pytest: ``python -m pytest evaluation/test_pickup.py``.
"""

from analyze import analyse_pickup
from runner import first_execute_at, parse_ts

LOG = """\
2026-10-06T15:59:11.076699769Z 2026-10-06 15:59:11,076 - INFO - common.sequential - Execute: method=GetConfiguration, task=task_8d5f6315
2026-10-06T15:59:11.077163412Z INFO:     172.18.0.2:55764 - "POST /control/execute HTTP/1.1" 200 OK
2026-10-06T15:59:11.722349534Z INFO:     172.18.0.3:55454 - "POST /control/execute HTTP/1.1" 422 Unprocessable Entity
2026-10-06T15:59:12.144557058Z 2026-10-06 15:59:12,144 - INFO - common.sequential - Execute: method=GetConfiguration, task=task_8d5f6315
"""


def test_first_execute_skips_earlier_workflows_with_the_same_task_id():
    created = parse_ts("2026-10-06T15:59:11.700000Z")
    assert abs(first_execute_at(LOG, "task_8d5f6315", created) - parse_ts("2026-10-06T15:59:12.144557Z")) < 1e-3


def test_first_execute_of_an_unknown_task_is_none():
    assert first_execute_at(LOG, "task_other", 0) is None


def trial(version, pickup, e2e, timeouts=0, jitter=0, gap=1.0):
    return {"scenario": "pickup", "use_case": "file_based", "version": version, "start_jitter": jitter,
            "pickup_seconds": pickup, "work_seconds": 0.04, "e2e_seconds": e2e,
            "since_previous_submit_seconds": gap, "worker_socket_timeouts_before": timeouts}


def test_pickup_cells_split_by_jitter_and_version_and_by_preceding_timeout():
    trials = [trial("old", 0.5, 0.6), trial("old", 0.2, 0.3, timeouts=1), trial("old", 0.5, 0.6),
              trial("new", 0.4, 0.5), trial("new", 0.45, 0.55),
              trial("new", 0.1, 0.2, jitter=1), {**trial("new", 0.1, 0.2), "warmup": True},
              {"scenario": "nominal", "version": "old", "use_case": "file_based", "e2e_seconds": 9}]
    r = analyse_pickup(trials)
    assert set(r["cells"]) == {"file_based/0/old", "file_based/0/new", "file_based/1/new"}
    old = r["cells"]["file_based/0/old"]
    assert old["n"] == 3 and old["socket_timeouts"] == 1
    assert old["pickup_median_after_timeout"] == 0.2 and old["pickup_median_no_timeout"] == 0.5
    assert set(r["comparisons"]) == {"file_based/0"}


def test_no_pickup_trials_no_section():
    assert analyse_pickup([{"scenario": "nominal", "version": "old", "use_case": "file_based"}]) == {}
