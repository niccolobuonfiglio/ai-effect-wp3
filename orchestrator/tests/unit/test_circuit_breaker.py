"""Unit tests for the circuit breaker."""

import json
import logging

import fakeredis
import pytest

from services.log_service import ALERTS_LOGGER
from services.circuit_breaker import (
    DEGRADED,
    HEALTHY,
    REGISTRY_KEY,
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
    UNHEALTHY,
    CircuitBreaker,
    CircuitOpenError,
    ServiceHealth,
)

SERVICE = "databroker"
BASE_URL = "http://databroker:8080"


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def breaker(redis_client):
    return CircuitBreaker(redis_client, failure_threshold=3, reset_timeout=60.0)


def fail(breaker, times, error="connection refused"):
    for _ in range(times):
        breaker.record_failure(SERVICE, BASE_URL, error)


class TestConstruction:
    def test_requires_redis_client(self):
        with pytest.raises(ValueError, match="redis_client is required"):
            CircuitBreaker(None)

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            ({"failure_threshold": 0}, "failure_threshold must be positive"),
            ({"failure_threshold": -1}, "failure_threshold must be positive"),
            ({"reset_timeout": 0}, "reset_timeout must be positive"),
            ({"degraded_threshold": 0}, "degraded_threshold must be positive"),
        ],
    )
    def test_rejects_invalid_config(self, redis_client, kwargs, match):
        with pytest.raises(ValueError, match=match):
            CircuitBreaker(redis_client, **kwargs)

    def test_defaults(self, redis_client):
        cb = CircuitBreaker(redis_client)
        assert cb._threshold == 3
        assert cb._reset_timeout == 60.0


class TestRegister:
    def test_register_makes_service_visible_as_healthy(self, breaker):
        breaker.register(SERVICE, BASE_URL)
        health = breaker.get_health(SERVICE)

        assert health is not None
        assert health.status == HEALTHY
        assert health.circuit_state == STATE_CLOSED
        assert health.failure_count == 0
        assert health.base_url == BASE_URL

    def test_register_is_idempotent_and_preserves_failures(self, breaker):
        fail(breaker, 2)
        breaker.register(SERVICE, BASE_URL)

        assert breaker.get_health(SERVICE).failure_count == 2

    def test_register_updates_base_url(self, breaker):
        breaker.register(SERVICE, BASE_URL)
        breaker.register(SERVICE, "http://databroker:9090")

        assert breaker.get_health(SERVICE).base_url == "http://databroker:9090"

    def test_register_rejects_blank_name(self, breaker):
        with pytest.raises(ValueError, match="service_name is required"):
            breaker.register("   ", BASE_URL)

    def test_unknown_service_has_no_health(self, breaker):
        assert breaker.get_health("nope") is None


class TestFailureAccumulation:
    def test_first_failure_degrades_but_allows_calls(self, breaker):
        fail(breaker, 1)
        health = breaker.get_health(SERVICE)

        assert health.status == DEGRADED
        assert health.circuit_state == STATE_CLOSED
        assert breaker.allow(SERVICE) is True

    def test_below_threshold_stays_closed(self, breaker):
        fail(breaker, 2)
        health = breaker.get_health(SERVICE)

        assert health.failure_count == 2
        assert health.circuit_state == STATE_CLOSED
        assert breaker.allow(SERVICE) is True

    def test_threshold_opens_circuit(self, breaker):
        fail(breaker, 3)
        health = breaker.get_health(SERVICE)

        assert health.status == UNHEALTHY
        assert health.circuit_state == STATE_OPEN
        assert breaker.allow(SERVICE) is False

    def test_open_circuit_records_failure_details(self, breaker):
        fail(breaker, 3, error="upstream timeout")
        health = breaker.get_health(SERVICE)

        assert health.last_error == "upstream timeout"
        assert health.last_failure is not None
        assert health.opened_at is not None
        assert health.retry_after is not None

    def test_retry_after_is_reset_timeout_past_open(self, redis_client):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=30.0)
        breaker.record_failure(SERVICE, BASE_URL, "boom")
        health = breaker.get_health(SERVICE)

        from datetime import datetime

        opened = datetime.fromisoformat(health.opened_at)
        retry = datetime.fromisoformat(health.retry_after)
        assert 29 <= (retry - opened).total_seconds() <= 31

    def test_failures_beyond_threshold_do_not_reopen(self, breaker):
        fail(breaker, 5)
        health = breaker.get_health(SERVICE)

        # Count keeps climbing but the circuit only opens once.
        assert health.failure_count == 5
        assert health.circuit_state == STATE_OPEN

    def test_closed_service_with_no_failures_is_healthy(self, breaker):
        breaker.register(SERVICE, BASE_URL)
        assert breaker.get_health(SERVICE).status == HEALTHY

    def test_rejects_blank_name(self, breaker):
        with pytest.raises(ValueError, match="service_name is required"):
            breaker.record_failure("", BASE_URL, "err")


class TestHalfOpenProbe:
    def test_open_circuit_blocks_before_timeout(self, redis_client):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=300.0)
        breaker.record_failure(SERVICE, BASE_URL, "down")

        assert breaker.allow(SERVICE) is False
        assert breaker.get_health(SERVICE).circuit_state == STATE_OPEN

    def test_elapsed_timeout_allows_one_probe(self, redis_client, monkeypatch):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=60.0)
        breaker.record_failure(SERVICE, BASE_URL, "down")

        # Jump past the reset window without sleeping.
        raw = json.loads(redis_client.hget(REGISTRY_KEY, SERVICE))
        raw["opened_ts"] -= 61
        redis_client.hset(REGISTRY_KEY, SERVICE, json.dumps(raw))

        assert breaker.allow(SERVICE) is True
        assert breaker.get_health(SERVICE).circuit_state == STATE_HALF_OPEN

    def test_half_open_still_reports_unhealthy(self, redis_client):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=60.0)
        breaker.record_failure(SERVICE, BASE_URL, "down")
        raw = json.loads(redis_client.hget(REGISTRY_KEY, SERVICE))
        raw["state"] = STATE_HALF_OPEN
        redis_client.hset(REGISTRY_KEY, SERVICE, json.dumps(raw))

        assert breaker.get_health(SERVICE).status == UNHEALTHY

    def test_failed_probe_reopens_immediately(self, redis_client):
        breaker = CircuitBreaker(redis_client, failure_threshold=5, reset_timeout=60.0)
        breaker.register(SERVICE, BASE_URL)
        raw = json.loads(redis_client.hget(REGISTRY_KEY, SERVICE))
        raw["state"] = STATE_HALF_OPEN
        redis_client.hset(REGISTRY_KEY, SERVICE, json.dumps(raw))

        # One failure re-opens even though the threshold is 5.
        breaker.record_failure(SERVICE, BASE_URL, "still down")

        assert breaker.get_health(SERVICE).circuit_state == STATE_OPEN
        assert breaker.allow(SERVICE) is False

    def test_successful_probe_closes_circuit(self, redis_client):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=60.0)
        breaker.record_failure(SERVICE, BASE_URL, "down")
        raw = json.loads(redis_client.hget(REGISTRY_KEY, SERVICE))
        raw["state"] = STATE_HALF_OPEN
        redis_client.hset(REGISTRY_KEY, SERVICE, json.dumps(raw))

        breaker.record_success(SERVICE, BASE_URL)
        health = breaker.get_health(SERVICE)

        assert health.status == HEALTHY
        assert health.circuit_state == STATE_CLOSED
        assert health.failure_count == 0


class TestCheck:
    def test_check_passes_when_closed(self, breaker):
        breaker.register(SERVICE, BASE_URL)
        breaker.check(SERVICE, BASE_URL)  # must not raise

    def test_check_raises_when_open(self, breaker):
        fail(breaker, 3)

        with pytest.raises(CircuitOpenError) as exc:
            breaker.check(SERVICE, BASE_URL)

        assert exc.value.service_name == SERVICE
        assert exc.value.base_url == BASE_URL
        assert "UNHEALTHY" in str(exc.value)


class TestSuccess:
    def test_success_clears_failure_count(self, breaker):
        fail(breaker, 2)
        breaker.record_success(SERVICE, BASE_URL)
        health = breaker.get_health(SERVICE)

        assert health.failure_count == 0
        assert health.status == HEALTHY
        assert health.last_success is not None

    def test_success_reopens_traffic_after_outage(self, breaker):
        fail(breaker, 3)
        assert breaker.allow(SERVICE) is False

        breaker.record_success(SERVICE, BASE_URL)

        assert breaker.allow(SERVICE) is True

    def test_consecutive_not_cumulative(self, breaker):
        """Successes reset the counter, so scattered failures never open it."""
        for _ in range(5):
            fail(breaker, 2)
            breaker.record_success(SERVICE, BASE_URL)

        assert breaker.get_health(SERVICE).circuit_state == STATE_CLOSED


class TestManualRecovery:
    def test_reset_closes_open_circuit(self, breaker):
        fail(breaker, 3)
        assert breaker.reset(SERVICE) is True

        health = breaker.get_health(SERVICE)
        assert health.status == HEALTHY
        assert health.circuit_state == STATE_CLOSED
        assert health.failure_count == 0
        assert health.manually_recovered_at is not None
        assert breaker.allow(SERVICE) is True

    def test_reset_unknown_service_returns_false(self, breaker):
        assert breaker.reset("ghost") is False

    def test_reset_preserves_last_success(self, breaker):
        breaker.record_success(SERVICE, BASE_URL)
        previous = breaker.get_health(SERVICE).last_success
        fail(breaker, 3)
        breaker.reset(SERVICE)

        assert breaker.get_health(SERVICE).last_success == previous

    def test_service_can_fail_again_after_recovery(self, breaker):
        fail(breaker, 3)
        breaker.reset(SERVICE)
        fail(breaker, 3)

        assert breaker.get_health(SERVICE).circuit_state == STATE_OPEN

    def test_reset_rejects_blank_name(self, breaker):
        with pytest.raises(ValueError, match="service_name is required"):
            breaker.reset("")


class TestListing:
    def test_empty_registry(self, breaker):
        assert breaker.get_all_health() == []

    def test_returns_all_sorted_by_name(self, breaker):
        breaker.register("zebra", "http://zebra:8080")
        breaker.register("alpha", "http://alpha:8080")
        breaker.register("mid", "http://mid:8080")

        assert [h.service_name for h in breaker.get_all_health()] == [
            "alpha",
            "mid",
            "zebra",
        ]

    def test_mixed_statuses(self, breaker):
        breaker.register("ok", "http://ok:8080")
        breaker.record_failure("warn", "http://warn:8080", "flaky")
        for _ in range(3):
            breaker.record_failure("bad", "http://bad:8080", "dead")

        statuses = {h.service_name: h.status for h in breaker.get_all_health()}
        assert statuses == {"ok": HEALTHY, "warn": DEGRADED, "bad": UNHEALTHY}

    def test_corrupt_entry_is_skipped(self, breaker, redis_client):
        breaker.register(SERVICE, BASE_URL)
        redis_client.hset(REGISTRY_KEY, "broken", "{not json")

        names = [h.service_name for h in breaker.get_all_health()]
        assert names == [SERVICE]

    def test_corrupt_entry_reads_as_absent(self, breaker, redis_client):
        redis_client.hset(REGISTRY_KEY, SERVICE, "{{{")
        assert breaker.get_health(SERVICE) is None

    def test_forget_removes_service(self, breaker):
        breaker.register(SERVICE, BASE_URL)

        assert breaker.forget(SERVICE) is True
        assert breaker.get_health(SERVICE) is None
        assert breaker.forget(SERVICE) is False


class TestSharedState:
    """State lives in Redis, so separate instances must agree."""

    def test_failures_visible_across_instances(self, redis_client):
        worker_a = CircuitBreaker(redis_client, failure_threshold=3)
        worker_b = CircuitBreaker(redis_client, failure_threshold=3)

        worker_a.record_failure(SERVICE, BASE_URL, "boom")
        worker_b.record_failure(SERVICE, BASE_URL, "boom")
        worker_a.record_failure(SERVICE, BASE_URL, "boom")

        assert worker_b.allow(SERVICE) is False
        assert worker_a.allow(SERVICE) is False

    def test_api_recovery_unblocks_workers(self, redis_client):
        worker = CircuitBreaker(redis_client, failure_threshold=1)
        api = CircuitBreaker(redis_client, failure_threshold=1)

        worker.record_failure(SERVICE, BASE_URL, "down")
        assert worker.allow(SERVICE) is False

        api.reset(SERVICE)

        assert worker.allow(SERVICE) is True


class TestAlerting:
    def test_service_down_alert_is_logged_once(self, breaker, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            fail(breaker, 5)

        alerts = [r for r in caplog.records if getattr(r, "alert", None) == "SERVICE_DOWN"]
        assert len(alerts) == 1

    def test_alert_message_format(self, breaker, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            fail(breaker, 3)

        record = next(r for r in caplog.records if getattr(r, "alert", None) == "SERVICE_DOWN")
        assert record.getMessage() == f"SERVICE DOWN: {SERVICE} at {BASE_URL}"

    def test_alert_carries_structured_fields_for_tef(self, breaker, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            fail(breaker, 3, error="upstream timeout")

        record = next(r for r in caplog.records if getattr(r, "alert", None) == "SERVICE_DOWN")
        assert record.event == "service_down"
        assert record.service_name == SERVICE
        assert record.base_url == BASE_URL
        assert record.failure_count == 3
        assert record.failure_threshold == 3
        assert record.last_error == "upstream timeout"
        assert record.timestamp

    def test_recovery_alert_logged_on_success_after_outage(self, breaker, caplog):
        fail(breaker, 3)
        with caplog.at_level(logging.INFO, logger=ALERTS_LOGGER):
            breaker.record_success(SERVICE, BASE_URL)

        assert any(
            getattr(r, "alert", None) == "SERVICE_RECOVERED" for r in caplog.records
        )

    def test_no_recovery_alert_when_never_down(self, breaker, caplog):
        with caplog.at_level(logging.INFO, logger=ALERTS_LOGGER):
            breaker.record_success(SERVICE, BASE_URL)

        assert not any(
            getattr(r, "alert", None) == "SERVICE_RECOVERED" for r in caplog.records
        )

    def test_manual_recovery_alert(self, breaker, caplog):
        fail(breaker, 3)
        with caplog.at_level(logging.WARNING, logger=ALERTS_LOGGER):
            breaker.reset(SERVICE)

        record = next(
            r for r in caplog.records
            if getattr(r, "alert", None) == "SERVICE_MANUAL_RECOVERY"
        )
        assert record.service_name == SERVICE

    def test_alert_schema_is_consistent(self, breaker, caplog):
        with caplog.at_level(logging.INFO, logger=ALERTS_LOGGER):
            fail(breaker, 3)
            breaker.reset(SERVICE)
            fail(breaker, 3)
            breaker.record_success(SERVICE, BASE_URL)

        alerts = [r for r in caplog.records if getattr(r, "alert", None)]
        assert [r.alert for r in alerts] == [
            "SERVICE_DOWN",
            "SERVICE_MANUAL_RECOVERY",
            "SERVICE_DOWN",
            "SERVICE_RECOVERED",
        ]
        for r in alerts:
            assert r.source == "orchestrator"
            assert r.severity in ("critical", "warning", "info")
            assert r.service_name == SERVICE
            assert r.base_url == BASE_URL
            assert r.timestamp

    def test_service_down_alert_written_as_json_line(self, breaker, tmp_path):
        from services.log_service import configure_logging

        root = logging.getLogger()
        saved = list(root.handlers), root.level
        try:
            configure_logging(log_dir=str(tmp_path), console=False)
            fail(breaker, 3, error="connection refused")
            for handler in logging.getLogger(ALERTS_LOGGER).handlers:
                handler.flush()

            lines = (tmp_path / "alerts.jsonl").read_text().splitlines()
        finally:
            for handler in root.handlers + logging.getLogger(ALERTS_LOGGER).handlers:
                handler.close()
            logging.getLogger(ALERTS_LOGGER).handlers.clear()
            root.handlers[:] = saved[0]
            root.setLevel(saved[1])

        assert len(lines) == 1
        alert = json.loads(lines[0])
        assert alert["message"] == f"SERVICE DOWN: {SERVICE} at {BASE_URL}"
        assert alert["alert"] == "SERVICE_DOWN"
        assert alert["severity"] == "critical"
        assert alert["level"] == "ERROR"
        assert alert["service_name"] == SERVICE
        assert alert["base_url"] == BASE_URL
        assert alert["failure_count"] == 3
        assert alert["last_error"] == "connection refused"


class TestServiceHealthModel:
    def test_to_dict_round_trip(self):
        health = ServiceHealth(
            service_name=SERVICE,
            base_url=BASE_URL,
            status=UNHEALTHY,
            circuit_state=STATE_OPEN,
            failure_count=3,
            last_error="boom",
        )
        data = health.to_dict()

        assert data["service_name"] == SERVICE
        assert data["failure_count"] == 3
        assert data["last_error"] == "boom"
        assert data["last_success"] is None


class TestErrorSanitising:
    def test_last_error_is_redacted(self, breaker):
        breaker.record_failure(SERVICE, BASE_URL, "failed for jane.doe@example.eu")

        assert "jane.doe@example.eu" not in breaker.get_health(SERVICE).last_error

    def test_last_error_is_capped(self, breaker):
        breaker.record_failure(SERVICE, BASE_URL, "y" * 3000)

        assert len(breaker.get_health(SERVICE).last_error) < 600

    def test_alert_carries_the_sanitised_error(self, breaker, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            fail(breaker, 3, error="boom for jane.doe@example.eu")

        record = next(r for r in caplog.records if getattr(r, "alert", None) == "SERVICE_DOWN")
        assert "jane.doe@example.eu" not in record.last_error


class TestProbeBackoff:
    """With initial_reset_timeout the wait before a probe grows while the
    service stays down, and starts over after it recovers."""

    @pytest.fixture
    def clock(self, monkeypatch):
        state = {"now": 1_000_000.0}
        monkeypatch.setattr("services.circuit_breaker.time.time", lambda: state["now"])
        return state

    @pytest.fixture
    def backoff_breaker(self, redis_client):
        return CircuitBreaker(
            redis_client, failure_threshold=1, reset_timeout=60, initial_reset_timeout=5
        )

    def open_circuit(self, breaker):
        breaker.record_failure(SERVICE, BASE_URL, "refused")

    def test_first_probe_after_initial_wait(self, backoff_breaker, clock):
        self.open_circuit(backoff_breaker)

        clock["now"] += 4.9
        assert backoff_breaker.allow(SERVICE) is False
        clock["now"] += 0.1
        assert backoff_breaker.allow(SERVICE) is True

    def test_wait_doubles_on_each_failed_probe(self, backoff_breaker, clock):
        self.open_circuit(backoff_breaker)
        waits = []
        for _ in range(6):
            opened = clock["now"]
            while not backoff_breaker.allow(SERVICE):
                clock["now"] += 1
            waits.append(clock["now"] - opened)
            backoff_breaker.record_failure(SERVICE, BASE_URL, "still refused")

        assert waits == [5, 10, 20, 40, 60, 60]

    def test_success_starts_over(self, backoff_breaker, clock):
        self.open_circuit(backoff_breaker)
        clock["now"] += 5
        backoff_breaker.allow(SERVICE)
        backoff_breaker.record_failure(SERVICE, BASE_URL, "still refused")  # streak 2

        clock["now"] += 10
        assert backoff_breaker.allow(SERVICE) is True
        backoff_breaker.record_success(SERVICE, BASE_URL)

        self.open_circuit(backoff_breaker)                                  # streak 1 again
        clock["now"] += 5
        assert backoff_breaker.allow(SERVICE) is True

    def test_manual_reset_starts_over(self, backoff_breaker, clock):
        self.open_circuit(backoff_breaker)
        clock["now"] += 5
        backoff_breaker.allow(SERVICE)
        backoff_breaker.record_failure(SERVICE, BASE_URL, "still refused")  # streak 2

        backoff_breaker.reset(SERVICE)
        self.open_circuit(backoff_breaker)
        clock["now"] += 5
        assert backoff_breaker.allow(SERVICE) is True

    def test_retry_after_reflects_current_wait(self, backoff_breaker, clock):
        from datetime import datetime, timezone

        self.open_circuit(backoff_breaker)
        retry_after = datetime.fromisoformat(backoff_breaker.get_health(SERVICE).retry_after)

        assert retry_after.timestamp() == pytest.approx(clock["now"] + 5)

    def test_without_initial_every_wait_is_reset_timeout(self, redis_client, clock):
        breaker = CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=60)
        breaker.record_failure(SERVICE, BASE_URL, "refused")

        clock["now"] += 59
        assert breaker.allow(SERVICE) is False
        clock["now"] += 1
        assert breaker.allow(SERVICE) is True

    def test_invalid_initial_rejected(self, redis_client):
        with pytest.raises(ValueError, match="initial_reset_timeout"):
            CircuitBreaker(redis_client, initial_reset_timeout=0)


class TestAlertDeduplication:
    """One outage emits one alert plus periodic reminders, not one per probe."""

    @pytest.fixture
    def clock(self, monkeypatch):
        state = {"now": 2_000_000.0}
        monkeypatch.setattr("services.circuit_breaker.time.time", lambda: state["now"])
        return state

    @pytest.fixture
    def breaker(self, redis_client):
        return CircuitBreaker(
            redis_client, failure_threshold=1, reset_timeout=60,
            initial_reset_timeout=5, alert_repeat_seconds=900,
        )

    def downs(self, caplog):
        return [r for r in caplog.records if getattr(r, "alert", None) == "SERVICE_DOWN"]

    def fail_probe_cycle(self, breaker, clock, times):
        """Let the circuit re-open ``times`` times, waiting for each probe."""
        for _ in range(times):
            while not breaker.allow(SERVICE):
                clock["now"] += 5
            breaker.record_failure(SERVICE, BASE_URL, "still refused")

    def test_one_alert_per_outage(self, breaker, clock, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            breaker.record_failure(SERVICE, BASE_URL, "refused")   # goes down
            self.fail_probe_cycle(breaker, clock, 5)               # 5 failed probes

        assert len(self.downs(caplog)) == 1

    def test_reminder_after_the_repeat_interval(self, breaker, clock, caplog):
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            breaker.record_failure(SERVICE, BASE_URL, "refused")
            clock["now"] += 901
            self.fail_probe_cycle(breaker, clock, 1)

        alerts = self.downs(caplog)
        assert len(alerts) == 2
        assert alerts[0].repeat is False and alerts[1].repeat is True
        assert alerts[1].open_count == 2
        assert alerts[1].down_since == alerts[0].down_since   # same outage

    def test_new_outage_alerts_again(self, breaker, clock, caplog):
        with caplog.at_level(logging.INFO, logger=ALERTS_LOGGER):
            breaker.record_failure(SERVICE, BASE_URL, "refused")
            clock["now"] += 5
            breaker.allow(SERVICE)
            breaker.record_success(SERVICE, BASE_URL)            # recovered
            breaker.record_failure(SERVICE, BASE_URL, "refused")  # down again

        alerts = self.downs(caplog)
        assert len(alerts) == 2
        assert alerts[1].repeat is False
        assert alerts[1].down_since != alerts[0].down_since

    def test_manual_recovery_starts_a_new_outage(self, breaker, clock, caplog):
        with caplog.at_level(logging.INFO, logger=ALERTS_LOGGER):
            breaker.record_failure(SERVICE, BASE_URL, "refused")
            breaker.reset(SERVICE)
            breaker.record_failure(SERVICE, BASE_URL, "refused")

        assert len(self.downs(caplog)) == 2

    def test_reminders_can_be_disabled(self, redis_client, clock, caplog):
        breaker = CircuitBreaker(
            redis_client, failure_threshold=1, reset_timeout=60,
            initial_reset_timeout=5, alert_repeat_seconds=0,
        )
        with caplog.at_level(logging.ERROR, logger=ALERTS_LOGGER):
            breaker.record_failure(SERVICE, BASE_URL, "refused")
            clock["now"] += 10_000
            self.fail_probe_cycle(breaker, clock, 3)

        assert len(self.downs(caplog)) == 1

    def test_health_is_unaffected_by_deduplication(self, breaker, clock):
        breaker.record_failure(SERVICE, BASE_URL, "refused")
        self.fail_probe_cycle(breaker, clock, 2)

        health = breaker.get_health(SERVICE)
        assert health.status == "UNHEALTHY"
        assert health.failure_count == 3

    def test_invalid_repeat_interval(self, redis_client):
        with pytest.raises(ValueError, match="alert_repeat_seconds"):
            CircuitBreaker(redis_client, alert_repeat_seconds=-1)
