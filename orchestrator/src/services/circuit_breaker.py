"""Redis-backed circuit breaker with service health tracking.

State is kept in a single Redis hash so that all worker replicas and the API
process share one view of service health: a failure observed by one worker
opens the circuit for every worker, and an operator-triggered recovery via the
API immediately unblocks all of them.
"""

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from redis import Redis

from services.log_service import get_alerts_logger
from services.privacy import sanitize_error

logger = logging.getLogger(__name__)
alerts_logger = get_alerts_logger()

ALERT_SOURCE = "orchestrator"

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
UNHEALTHY = "UNHEALTHY"

STATE_CLOSED = "closed"
STATE_OPEN = "open"
STATE_HALF_OPEN = "half_open"

REGISTRY_KEY = "circuit:registry"


@dataclass(frozen=True)
class ServiceHealth:
    """Point-in-time health snapshot for a single service."""

    service_name: str
    base_url: str
    status: str
    circuit_state: str
    failure_count: int
    last_failure: str | None = None
    last_error: str | None = None
    last_success: str | None = None
    opened_at: str | None = None
    retry_after: str | None = None
    manually_recovered_at: str | None = None

    def to_dict(self) -> dict:
        """Plain dict, suitable for building API responses."""
        return {
            "service_name": self.service_name,
            "base_url": self.base_url,
            "status": self.status,
            "circuit_state": self.circuit_state,
            "failure_count": self.failure_count,
            "last_failure": self.last_failure,
            "last_error": self.last_error,
            "last_success": self.last_success,
            "opened_at": self.opened_at,
            "retry_after": self.retry_after,
            "manually_recovered_at": self.manually_recovered_at,
        }


class CircuitOpenError(Exception):
    """Raised when a call is attempted against an open circuit."""

    def __init__(self, service_name: str, base_url: str):
        self.service_name = service_name
        self.base_url = base_url
        super().__init__(
            f"Circuit open for service '{service_name}' at {base_url} - "
            f"service marked UNHEALTHY, call not attempted"
        )


class CircuitBreaker:
    """Tracks per-service failures in Redis; opens after N consecutive failures.

    Args:
        redis_client: Shared Redis connection.
        failure_threshold: Consecutive failures before the circuit opens.
        reset_timeout: Seconds before an open circuit allows one probe call.
        degraded_threshold: Failure count at which a closed circuit reports
            DEGRADED rather than HEALTHY.
    """

    def __init__(
        self,
        redis_client: Redis,
        failure_threshold: int = 3,
        reset_timeout: float = 60.0,
        degraded_threshold: int = 1,
    ):
        if redis_client is None:
            raise ValueError("redis_client is required")
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if reset_timeout <= 0:
            raise ValueError("reset_timeout must be positive")
        if degraded_threshold <= 0:
            raise ValueError("degraded_threshold must be positive")

        self._redis = redis_client
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._degraded_threshold = degraded_threshold

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _load(self, service_name: str) -> dict:
        raw = self._redis.hget(REGISTRY_KEY, service_name)
        if raw is None:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return {}

    def _save(self, service_name: str, data: dict) -> None:
        self._redis.hset(REGISTRY_KEY, service_name, json.dumps(data))

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _alert(
        level: int,
        alert: str,
        severity: str,
        service_name: str,
        base_url: str,
        timestamp: str,
        **fields,
    ) -> None:
        """Emit a structured alert in the schema forwarded to TEF monitoring."""
        label = alert.replace("_", " ")
        alerts_logger.log(
            level,
            "%s: %s at %s",
            label,
            service_name,
            base_url,
            extra={
                "event": alert.lower(),
                "alert": alert,
                "severity": severity,
                "source": ALERT_SOURCE,
                "service_name": service_name,
                "base_url": base_url,
                "timestamp": timestamp,
                **fields,
            },
        )

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def register(self, service_name: str, base_url: str) -> None:
        """Register a service so it appears in health listings before any call."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        data = self._load(service_name)
        data.setdefault("failure_count", 0)
        data.setdefault("state", STATE_CLOSED)
        data["base_url"] = base_url
        self._save(service_name, data)

    def allow(self, service_name: str) -> bool:
        """Return True if calls to this service are permitted right now.

        An open circuit whose reset timeout has elapsed transitions to
        half-open and permits a single probe call.
        """
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        data = self._load(service_name)
        if data.get("state") != STATE_OPEN:
            return True

        opened_ts = float(data.get("opened_ts", 0))
        if time.time() - opened_ts >= self._reset_timeout:
            data["state"] = STATE_HALF_OPEN
            self._save(service_name, data)
            logger.info(
                "Circuit half-open, probing: %s at %s",
                service_name,
                data.get("base_url", ""),
                extra={
                    "event": "circuit_half_open",
                    "service_name": service_name,
                    "base_url": data.get("base_url", ""),
                },
            )
            return True

        return False

    def check(self, service_name: str, base_url: str) -> None:
        """Raise CircuitOpenError if the circuit for this service is open."""
        if not self.allow(service_name):
            raise CircuitOpenError(service_name, base_url)

    def record_success(self, service_name: str, base_url: str) -> None:
        """Reset failure state after a successful call."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        data = self._load(service_name)
        was_open = data.get("state") in (STATE_OPEN, STATE_HALF_OPEN)

        self._save(
            service_name,
            {
                "base_url": base_url,
                "state": STATE_CLOSED,
                "failure_count": 0,
                "last_success": self._now(),
                "manually_recovered_at": data.get("manually_recovered_at"),
            },
        )

        if was_open:
            self._alert(
                logging.INFO,
                "SERVICE_RECOVERED",
                "info",
                service_name,
                base_url,
                self._now(),
            )

    def record_failure(self, service_name: str, base_url: str, error: str) -> None:
        """Record a failed call, opening the circuit once the threshold is hit."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        error = sanitize_error(error)
        data = self._load(service_name)
        count = int(data.get("failure_count", 0)) + 1
        now = self._now()

        data.update(
            {
                "base_url": base_url,
                "failure_count": count,
                "last_failure": now,
                "last_error": error,
            }
        )

        should_open = (
            count >= self._threshold or data.get("state") == STATE_HALF_OPEN
        ) and data.get("state") != STATE_OPEN

        if should_open:
            data["state"] = STATE_OPEN
            data["opened_ts"] = time.time()
            data["opened_at"] = now
            self._save(service_name, data)
            self._alert(
                logging.ERROR,
                "SERVICE_DOWN",
                "critical",
                service_name,
                base_url,
                now,
                failure_count=count,
                failure_threshold=self._threshold,
                last_error=error,
            )
        else:
            self._save(service_name, data)
            logger.warning(
                "Service call failed (%d/%d): %s at %s - %s",
                count,
                self._threshold,
                service_name,
                base_url,
                error,
                extra={
                    "event": "service_call_failed",
                    "service_name": service_name,
                    "base_url": base_url,
                    "failure_count": count,
                    "last_error": error,
                    "timestamp": now,
                },
            )

    def reset(self, service_name: str) -> bool:
        """Manually close the circuit. Returns False if the service is unknown."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        data = self._load(service_name)
        if not data:
            return False

        base_url = data.get("base_url", "")
        now = self._now()
        self._save(
            service_name,
            {
                "base_url": base_url,
                "state": STATE_CLOSED,
                "failure_count": 0,
                "last_success": data.get("last_success"),
                "manually_recovered_at": now,
            },
        )

        self._alert(
            logging.WARNING,
            "SERVICE_MANUAL_RECOVERY",
            "warning",
            service_name,
            base_url,
            now,
        )
        return True

    def get_health(self, service_name: str) -> ServiceHealth | None:
        """Health snapshot for one service, or None if unregistered."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        data = self._load(service_name)
        if not data:
            return None
        return self._to_health(service_name, data)

    def get_all_health(self) -> list[ServiceHealth]:
        """Health snapshots for every registered service, sorted by name."""
        entries = self._redis.hgetall(REGISTRY_KEY) or {}
        out: list[ServiceHealth] = []

        for name, raw in entries.items():
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            out.append(self._to_health(name, data))

        return sorted(out, key=lambda h: h.service_name)

    def forget(self, service_name: str) -> bool:
        """Remove a service from the registry entirely."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")
        return bool(self._redis.hdel(REGISTRY_KEY, service_name))

    # ------------------------------------------------------------------
    # mapping
    # ------------------------------------------------------------------
    def _to_health(self, name: str, data: dict) -> ServiceHealth:
        count = int(data.get("failure_count", 0))
        state = data.get("state", STATE_CLOSED)

        if state in (STATE_OPEN, STATE_HALF_OPEN):
            status = UNHEALTHY
        elif count >= self._degraded_threshold:
            status = DEGRADED
        else:
            status = HEALTHY

        retry_after = None
        if state == STATE_OPEN and "opened_ts" in data:
            retry_after = datetime.fromtimestamp(
                float(data["opened_ts"]) + self._reset_timeout, tz=timezone.utc
            ).isoformat()

        return ServiceHealth(
            service_name=name,
            base_url=data.get("base_url", ""),
            status=status,
            circuit_state=state,
            failure_count=count,
            last_failure=data.get("last_failure"),
            last_error=data.get("last_error"),
            last_success=data.get("last_success"),
            opened_at=data.get("opened_at"),
            retry_after=retry_after,
            manually_recovered_at=data.get("manually_recovered_at"),
        )
