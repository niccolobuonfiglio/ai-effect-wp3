"""Unit tests for pruning and keep-alive of per-process log files."""

import os
import time

import pytest

from services import log_retention
from services.log_retention import (
    is_stale,
    keepalive_paths,
    prune_stale_files,
    retention_days,
    start_keepalive,
    touch,
)

DAY = 86400


def make(path, age_days=0.0):
    path.write_text("x\n")
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))
    return path


class TestRetentionDays:
    def test_default(self, monkeypatch):
        monkeypatch.delenv("LOG_RETENTION_DAYS", raising=False)
        assert retention_days() == 7.0

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("LOG_RETENTION_DAYS", "2.5")
        assert retention_days() == 2.5

    @pytest.mark.parametrize("raw", ["abc", "0", "-1"])
    def test_invalid(self, monkeypatch, raw):
        monkeypatch.setenv("LOG_RETENTION_DAYS", raw)
        with pytest.raises(ValueError, match="LOG_RETENTION_DAYS"):
            retention_days()


class TestIsStale:
    def test_old_file(self, tmp_path):
        assert is_stale(make(tmp_path / "a.log", age_days=8), 7) is True

    def test_fresh_file(self, tmp_path):
        assert is_stale(make(tmp_path / "a.log", age_days=1), 7) is False

    def test_missing_file_is_not_stale(self, tmp_path):
        assert is_stale(tmp_path / "missing.log", 7) is False


class TestPrune:
    def test_removes_only_stale_per_process_files(self, tmp_path):
        gone = [
            make(tmp_path / "worker-dead1.log", 10),
            make(tmp_path / "worker-dead1.log.2026-09-01", 10),
            make(tmp_path / "alerts-worker-dead1.jsonl", 10),
            make(tmp_path / "alerts-api-dead2.jsonl", 10),
            make(tmp_path / "orchestrator.log.2026-08-28", 10),
            make(tmp_path / "worker_daemon.log", 10),   # old naming
            make(tmp_path / "alerts.jsonl", 10),         # old naming
        ]
        kept = [
            make(tmp_path / "worker-live.log", 1),
            make(tmp_path / "alerts-worker-live.jsonl", 0),
            make(tmp_path / "orchestrator.log", 30),     # the API's current file
            make(tmp_path / "notes.txt", 30),            # not a log file
        ]

        deleted = prune_stale_files(tmp_path, max_age_days=7)

        assert sorted(p.name for p in deleted) == sorted(p.name for p in gone)
        assert all(not p.exists() for p in gone)
        assert all(p.exists() for p in kept)

    def test_own_files_are_never_deleted(self, tmp_path):
        own = make(tmp_path / "worker-me.log", 30)

        prune_stale_files(tmp_path, max_age_days=7, keep={"worker-me.log"})

        assert own.exists()

    def test_retention_from_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOG_RETENTION_DAYS", "1")
        old = make(tmp_path / "worker-x.log", 2)

        prune_stale_files(tmp_path)

        assert not old.exists()

    def test_missing_directory(self, tmp_path):
        assert prune_stale_files(tmp_path / "absent", max_age_days=7) == []


class TestKeepalive:
    def test_touch_refreshes_mtime(self, tmp_path):
        path = make(tmp_path / "worker-me.log", 10)

        touch([path, tmp_path / "missing.log"])

        assert not is_stale(path, 7)

    def test_start_registers_paths_and_runs_one_thread(self, tmp_path):
        start_keepalive([tmp_path / "a.log"])
        start_keepalive([tmp_path / "b.log"])

        assert keepalive_paths() == [tmp_path / "b.log"]
        assert log_retention._keepalive_thread.is_alive()
        assert log_retention._keepalive_thread.daemon is True

    def test_invalid_interval(self, tmp_path):
        with pytest.raises(ValueError, match="interval"):
            start_keepalive([tmp_path / "a.log"], interval=0)
