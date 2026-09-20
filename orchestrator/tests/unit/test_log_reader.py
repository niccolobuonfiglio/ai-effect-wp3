"""Unit tests for the shared-logs reader."""

import json

import pytest

from services.log_reader import LINE_CHARS, LogReader, clamp


def write(path, lines):
    path.write_text("\n".join(lines) + "\n")


@pytest.fixture
def logs(tmp_path):
    write(tmp_path / "orchestrator.log", [
        "2026-09-18 23:23:01,000 - INFO - __main__ - POST /workflows wf-aaa",
        "2026-09-18 23:23:05,000 - INFO - api - unrelated",
    ])
    write(tmp_path / "worker-w1.log", [
        "2026-09-18 23:23:03,000 - WARNING - services.worker - Task t1 of workflow wf-aaa blocked",
        "2026-09-18 23:23:04,000 - ERROR - worker_daemon - Error processing workflow wf-bbb: boom",
        "Traceback (most recent call last):",
        '  File "x.py", line 1',
    ])
    write(tmp_path / "worker-w2.log", [
        "2026-09-18 23:23:02,000 - INFO - worker_daemon - Processed task for workflow wf-aaa",
    ])
    # Rotated copy: must be ignored.
    write(tmp_path / "worker-w1.log.2026-09-17", [
        "2026-09-17 10:00:00,000 - INFO - old - wf-aaa from yesterday",
    ])
    return tmp_path


class TestWorkflowLog:
    def test_merges_files_in_time_order(self, logs):
        records, matched = LogReader(logs).workflow_log("wf-aaa")

        assert matched is True
        assert [r[11:19] for r in records] == ["23:23:01", "23:23:02", "23:23:03"]

    def test_ignores_rotated_files(self, logs):
        records, _ = LogReader(logs).workflow_log("wf-aaa")
        assert not any("yesterday" in r for r in records)

    def test_keeps_multiline_records_together(self, logs):
        records, _ = LogReader(logs).workflow_log("wf-bbb")

        assert len(records) == 1
        assert "Traceback" in records[0] and 'File "x.py"' in records[0]

    def test_falls_back_to_recent_lines(self, logs):
        records, matched = LogReader(logs).workflow_log("wf-none", lines=2)

        assert matched is False
        assert len(records) == 2
        assert "unrelated" in records[-1]

    def test_limits_line_count(self, logs):
        records, _ = LogReader(logs).workflow_log("wf-aaa", lines=1)
        assert len(records) == 1 and "23:23:03" in records[0]

    def test_redacts_and_caps_lines(self, tmp_path):
        write(tmp_path / "worker-w1.log", [
            "2026-09-18 23:23:03,000 - WARNING - x - wf-ccc failed for jane.doe@example.eu " + "y" * 5000,
        ])
        records, _ = LogReader(tmp_path).workflow_log("wf-ccc")

        assert "jane.doe@example.eu" not in records[0]
        assert len(records[0]) < LINE_CHARS + 100

    def test_reads_only_the_tail(self, tmp_path):
        old = [f"2026-09-18 10:00:{i % 60:02d},000 - INFO - x - old line {i}" for i in range(2000)]
        write(tmp_path / "orchestrator.log", old + ["2026-09-18 23:59:59,000 - INFO - x - newest wf-ddd"])

        reader = LogReader(tmp_path, tail_bytes=2048)
        records, _ = reader.workflow_log("wf-ddd")
        everything, _ = reader.workflow_log("wf-none", lines=1000)

        assert "newest" in records[0]
        assert len(everything) < 50            # only ~2 KB were read
        assert all(r.startswith("2026") for r in everything)  # no half line

    def test_json_log_lines(self, tmp_path):
        write(tmp_path / "orchestrator.log", [
            json.dumps({"timestamp": "2026-09-18T23:23:05+00:00", "message": "late wf-eee"}),
            json.dumps({"timestamp": "2026-09-18T23:23:01+00:00", "message": "early wf-eee"}),
        ])
        records, _ = LogReader(tmp_path).workflow_log("wf-eee")

        assert ["early" in records[0], "late" in records[1]] == [True, True]

    def test_missing_directory(self, tmp_path):
        reader = LogReader(tmp_path / "absent")

        assert reader.directory() is None
        assert reader.workflow_log("wf-1") == ([], False)
        assert reader.alerts() == []

    def test_log_dir_from_env(self, logs, monkeypatch):
        monkeypatch.setenv("LOG_DIR", str(logs))
        assert LogReader().directory() == logs

    def test_validation(self, logs):
        with pytest.raises(ValueError, match="workflow_id"):
            LogReader(logs).workflow_log("")
        with pytest.raises(ValueError, match="tail_bytes"):
            LogReader(logs, tail_bytes=0)


class TestStaleFiles:
    """Files of processes that are gone are not read."""

    def age(self, path, days):
        import os
        import time

        stamp = time.time() - days * 86400
        os.utime(path, (stamp, stamp))

    def test_stale_log_file_skipped(self, tmp_path):
        write(tmp_path / "worker-dead.log", ["2026-09-18 10:00:00,000 - INFO - x - wf-zzz old"])
        write(tmp_path / "worker-live.log", ["2026-09-18 11:00:00,000 - INFO - x - wf-zzz new"])
        self.age(tmp_path / "worker-dead.log", 30)

        records, _ = LogReader(tmp_path, max_age_days=7).workflow_log("wf-zzz")

        assert len(records) == 1 and "new" in records[0]

    def test_stale_alert_file_skipped(self, tmp_path):
        write(tmp_path / "alerts-worker-dead.jsonl", [json.dumps({"timestamp": "2026-09-18T10:00:00", "alert": "SERVICE_DOWN"})])
        self.age(tmp_path / "alerts-worker-dead.jsonl", 30)

        assert LogReader(tmp_path, max_age_days=7).alerts() == []

    def test_invalid_max_age(self, tmp_path):
        with pytest.raises(ValueError, match="max_age_days"):
            LogReader(tmp_path, max_age_days=0)


class TestAlerts:
    def alert(self, ts, kind, service, error=None):
        data = {"timestamp": ts, "alert": kind, "service_name": service}
        if error:
            data["last_error"] = error
        return json.dumps(data)

    def test_merges_replicas_newest_first(self, tmp_path):
        write(tmp_path / "alerts-worker-w1.jsonl", [
            self.alert("2026-09-18T23:23:09+00:00", "SERVICE_DOWN", "svc"),
        ])
        write(tmp_path / "alerts-worker-w2.jsonl", [
            self.alert("2026-09-18T23:25:05+00:00", "SERVICE_RECOVERED", "svc"),
        ])
        write(tmp_path / "alerts-api-a1.jsonl", [
            self.alert("2026-09-18T23:24:00+00:00", "SERVICE_MANUAL_RECOVERY", "svc"),
        ])

        kinds = [a["alert"] for a in LogReader(tmp_path).alerts()]

        assert kinds == ["SERVICE_RECOVERED", "SERVICE_MANUAL_RECOVERY", "SERVICE_DOWN"]

    def test_limit_and_bad_lines(self, tmp_path):
        write(tmp_path / "alerts-worker-w1.jsonl", [
            "not json", "[1, 2]",
            *[self.alert(f"2026-09-18T23:23:{i:02d}+00:00", "SERVICE_DOWN", "svc") for i in range(10)],
        ])

        alerts = LogReader(tmp_path).alerts(limit=3)

        assert len(alerts) == 3
        assert alerts[0]["timestamp"].startswith("2026-09-18T23:23:09")

    def test_rotated_alert_files_ignored(self, tmp_path):
        write(tmp_path / "alerts-worker-w1.jsonl.2026-09-17", [
            self.alert("2026-09-17T10:00:00+00:00", "SERVICE_DOWN", "old"),
        ])
        assert LogReader(tmp_path).alerts() == []

    def test_errors_in_alerts_are_redacted(self, tmp_path):
        write(tmp_path / "alerts-worker-w1.jsonl", [
            self.alert("2026-09-18T23:23:09+00:00", "SERVICE_DOWN", "svc", "down for jane.doe@example.eu"),
        ])
        assert "jane.doe@example.eu" not in LogReader(tmp_path).alerts()[0]["last_error"]


def test_clamp():
    assert [clamp(0), clamp(5), clamp(10_000)] == [1, 5, 1000]
