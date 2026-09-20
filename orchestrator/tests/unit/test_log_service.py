"""Unit tests for logging configuration."""

import json
import logging

import pytest

from services.log_service import (
    ALERTS_LOGGER,
    JsonFormatter,
    configure_logging,
    get_alerts_logger,
)


@pytest.fixture
def restore_logging():
    root = logging.getLogger()
    alerts = get_alerts_logger()
    saved = list(root.handlers), root.level
    yield
    for handler in root.handlers + alerts.handlers:
        handler.close()
    alerts.handlers.clear()
    root.handlers[:] = saved[0]
    root.setLevel(saved[1])


def make_record(msg="hello %s", args=("world",), **extra):
    record = logging.LogRecord("test.logger", logging.WARNING, __file__, 1, msg, args, None)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


class TestJsonFormatter:
    def test_standard_fields(self):
        data = json.loads(JsonFormatter().format(make_record()))

        assert data["message"] == "hello world"
        assert data["level"] == "WARNING"
        assert data["logger"] == "test.logger"
        assert data["timestamp"]

    def test_extra_fields_are_top_level(self):
        data = json.loads(
            JsonFormatter().format(make_record(service_name="svc", failure_count=2))
        )

        assert data["service_name"] == "svc"
        assert data["failure_count"] == 2

    def test_internal_record_attributes_are_omitted(self):
        data = json.loads(JsonFormatter().format(make_record()))

        for key in ("args", "msg", "levelno", "pathname", "lineno", "created"):
            assert key not in data

    def test_extra_timestamp_overrides_record_time(self):
        data = json.loads(
            JsonFormatter().format(make_record(timestamp="2026-01-01T00:00:00+00:00"))
        )

        assert data["timestamp"] == "2026-01-01T00:00:00+00:00"

    def test_non_serializable_values_are_stringified(self):
        data = json.loads(JsonFormatter().format(make_record(obj=object())))

        assert data["obj"].startswith("<object object")

    def test_one_line_per_record(self):
        out = JsonFormatter().format(make_record(msg="line1\nline2", args=()))

        assert "\n" not in out


class TestConfigureLogging:
    def test_text_format_by_default(self, tmp_path, restore_logging, monkeypatch):
        monkeypatch.delenv("LOG_FORMAT", raising=False)
        root = configure_logging(log_dir=str(tmp_path), console=False)

        assert not isinstance(root.handlers[0].formatter, JsonFormatter)

    def test_json_format_from_env(self, tmp_path, restore_logging, monkeypatch):
        monkeypatch.setenv("LOG_FORMAT", "json")
        root = configure_logging(log_dir=str(tmp_path), console=False)

        assert isinstance(root.handlers[0].formatter, JsonFormatter)

    def test_invalid_format_rejected(self, tmp_path, restore_logging):
        with pytest.raises(ValueError, match="log_format"):
            configure_logging(log_dir=str(tmp_path), console=False, log_format="xml")

    def test_alerts_file_receives_only_alerts(self, tmp_path, restore_logging):
        configure_logging(log_dir=str(tmp_path), console=False, log_format="text")

        logging.getLogger("some.module").error("not an alert")
        logging.getLogger(ALERTS_LOGGER).error("an alert", extra={"alert": "X"})
        for handler in logging.getLogger().handlers + get_alerts_logger().handlers:
            handler.flush()

        alert_lines = (tmp_path / "alerts.jsonl").read_text().splitlines()
        assert [json.loads(line)["message"] for line in alert_lines] == ["an alert"]

        # Alerts still reach the main log too.
        main_log = (tmp_path / "orchestrator.log").read_text()
        assert "not an alert" in main_log
        assert "an alert" in main_log

    def test_alerts_file_can_be_disabled(self, tmp_path, restore_logging):
        configure_logging(log_dir=str(tmp_path), console=False, alerts_file=None)

        assert get_alerts_logger().handlers == []

    def test_reconfiguring_does_not_duplicate_alert_handlers(self, tmp_path, restore_logging):
        configure_logging(log_dir=str(tmp_path), console=False)
        configure_logging(log_dir=str(tmp_path), console=False)

        assert len(get_alerts_logger().handlers) == 1

    def test_prunes_files_of_processes_that_are_gone(self, tmp_path, restore_logging):
        import os
        import time

        stale = tmp_path / "worker-dead.log"
        stale.write_text("old\n")
        old = time.time() - 30 * 86400
        os.utime(stale, (old, old))

        configure_logging(log_dir=str(tmp_path), log_file="worker-me.log",
                          alerts_file="alerts-worker-me.jsonl", console=False)

        assert not stale.exists()
        assert (tmp_path / "worker-me.log").exists()

    def test_keeps_own_files_alive(self, tmp_path, restore_logging):
        from services.log_retention import keepalive_paths

        configure_logging(log_dir=str(tmp_path), log_file="worker-me.log",
                          alerts_file="alerts-worker-me.jsonl", console=False)

        assert keepalive_paths() == [tmp_path / "worker-me.log", tmp_path / "alerts-worker-me.jsonl"]

    def test_retention_can_be_turned_off(self, tmp_path, restore_logging):
        import os
        import time

        stale = tmp_path / "worker-dead.log"
        stale.write_text("old\n")
        old = time.time() - 30 * 86400
        os.utime(stale, (old, old))

        configure_logging(log_dir=str(tmp_path), console=False, manage_retention=False)

        assert stale.exists()

