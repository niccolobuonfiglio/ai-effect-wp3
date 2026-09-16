"""Registry of services known to the orchestrator.

Sits between the parsed ``dockerinfo.json`` endpoints and the circuit breaker.
It owns two concerns the breaker deliberately does not:

* **Endpoint resolution** - which ``base_url`` a container name maps to, and
  which workflows referenced it. Workers ask the registry, not the raw
  ``endpoints:{workflow_id}`` hash.
* **Aggregation** - overall status and per-status counts across every known
  service, for the ops dashboard.

Health state itself (failure counts, circuit state) remains the circuit
breaker's responsibility; the registry delegates to it so there is exactly one
source of truth.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from redis import Redis

from services.circuit_breaker import (
    DEGRADED,
    HEALTHY,
    UNHEALTHY,
    CircuitBreaker,
    ServiceHealth,
)
from services.dockerinfo_parser import ServiceEndpoint
from services.state_store import TaskNotFoundError, WorkflowNotFoundError

if TYPE_CHECKING:
    from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)

REGISTRY_KEY = "registry:services"


class ServiceNotFoundError(Exception):
    """Raised when a service is not present in the registry."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        super().__init__(f"Service not registered: {service_name}")


@dataclass(frozen=True)
class RegisteredService:
    """A service the orchestrator knows how to reach."""

    service_name: str
    address: str
    port: int
    base_url: str
    first_seen: str
    last_seen: str
    workflow_ids: list[str]

    def to_dict(self) -> dict:
        return {
            "service_name": self.service_name,
            "address": self.address,
            "port": self.port,
            "base_url": self.base_url,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "workflow_ids": list(self.workflow_ids),
        }


@dataclass(frozen=True)
class RegistrySummary:
    """Aggregate view across all registered services."""

    overall_status: str
    total: int
    healthy: int
    degraded: int
    unhealthy: int
    services: list[ServiceHealth]


class ServiceRegistry:
    """Tracks known services and exposes their aggregated health."""

    MAX_WORKFLOW_IDS = 20

    def __init__(
        self,
        redis_client: Redis,
        breaker: CircuitBreaker,
        engine: "WorkflowEngine | None" = None,
    ):
        """
        Args:
            redis_client: Shared Redis connection.
            breaker: Circuit breaker holding per-service health.
            engine: Workflow engine used to resume tasks blocked on a service
                after manual recovery. Without it, recovery only resets health.
        """
        if redis_client is None:
            raise ValueError("redis_client is required")
        if breaker is None:
            raise ValueError("breaker is required")

        self._redis = redis_client
        self._breaker = breaker
        self._engine = engine

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

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
    def _base_url(address: str, port: int) -> str:
        return f"http://{address}:{port}"

    # ------------------------------------------------------------------
    # registration
    # ------------------------------------------------------------------
    def register(
        self,
        service_name: str,
        endpoint: ServiceEndpoint,
        workflow_id: str | None = None,
    ) -> RegisteredService:
        """Register (or refresh) a single service endpoint."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")
        if endpoint is None:
            raise ValueError("endpoint is required")

        now = self._now()
        base_url = self._base_url(endpoint.address, endpoint.port)
        data = self._load(service_name)

        workflow_ids: list[str] = list(data.get("workflow_ids", []))
        if workflow_id and workflow_id not in workflow_ids:
            workflow_ids.append(workflow_id)
            # Keep only the most recent N so the record cannot grow unbounded.
            workflow_ids = workflow_ids[-self.MAX_WORKFLOW_IDS :]

        record = {
            "service_name": service_name,
            "address": endpoint.address,
            "port": endpoint.port,
            "base_url": base_url,
            "first_seen": data.get("first_seen", now),
            "last_seen": now,
            "workflow_ids": workflow_ids,
        }
        self._save(service_name, record)

        # Mirror into the breaker so the service appears in health listings
        # before any task has run against it.
        self._breaker.register(service_name, base_url)

        logger.debug(
            "Registered service %s at %s", service_name, base_url,
            extra={
                "event": "service_registered",
                "service_name": service_name,
                "base_url": base_url,
                "workflow_id": workflow_id,
            },
        )
        return RegisteredService(**record)

    def register_all(
        self,
        endpoints: dict[str, ServiceEndpoint],
        workflow_id: str | None = None,
    ) -> list[RegisteredService]:
        """Register every endpoint parsed from a dockerinfo document."""
        if endpoints is None:
            raise ValueError("endpoints is required")
        return [
            self.register(name, endpoint, workflow_id)
            for name, endpoint in sorted(endpoints.items())
        ]

    # ------------------------------------------------------------------
    # lookup
    # ------------------------------------------------------------------
    def get(self, service_name: str) -> RegisteredService | None:
        """Return the registered service, or None if unknown."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")
        data = self._load(service_name)
        if not data:
            return None
        return RegisteredService(**data)

    def get_or_raise(self, service_name: str) -> RegisteredService:
        """Return the registered service, raising if unknown."""
        service = self.get(service_name)
        if service is None:
            raise ServiceNotFoundError(service_name)
        return service

    def list_services(self) -> list[RegisteredService]:
        """All registered services, sorted by name."""
        entries = self._redis.hgetall(REGISTRY_KEY) or {}
        out: list[RegisteredService] = []
        for name, raw in entries.items():
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                out.append(RegisteredService(**json.loads(raw)))
            except (ValueError, TypeError):
                continue
        return sorted(out, key=lambda s: s.service_name)

    def base_url_for(self, service_name: str) -> str:
        """Resolve a service name to its base URL."""
        return self.get_or_raise(service_name).base_url

    def is_registered(self, service_name: str) -> bool:
        return self.get(service_name) is not None

    # ------------------------------------------------------------------
    # health
    # ------------------------------------------------------------------
    def get_health(self, service_name: str) -> ServiceHealth | None:
        """Health of a single service, or None if unknown to both stores."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        health = self._breaker.get_health(service_name)
        if health is not None:
            return health

        # Registered but never exercised: report it as healthy rather than
        # hiding it from the dashboard.
        service = self.get(service_name)
        if service is None:
            return None
        self._breaker.register(service_name, service.base_url)
        return self._breaker.get_health(service_name)

    def get_summary(self) -> RegistrySummary:
        """Aggregate health across every known service."""
        # Union of both stores: a service may be registered but not yet
        # exercised, or exercised under a name registered by another replica.
        health_by_name = {h.service_name: h for h in self._breaker.get_all_health()}

        for service in self.list_services():
            if service.service_name not in health_by_name:
                self._breaker.register(service.service_name, service.base_url)
                health = self._breaker.get_health(service.service_name)
                if health is not None:
                    health_by_name[service.service_name] = health

        services = sorted(health_by_name.values(), key=lambda h: h.service_name)

        counts = {HEALTHY: 0, DEGRADED: 0, UNHEALTHY: 0}
        for health in services:
            counts[health.status] = counts.get(health.status, 0) + 1

        if counts[UNHEALTHY]:
            overall = UNHEALTHY
        elif counts[DEGRADED]:
            overall = DEGRADED
        else:
            overall = HEALTHY

        return RegistrySummary(
            overall_status=overall,
            total=len(services),
            healthy=counts[HEALTHY],
            degraded=counts[DEGRADED],
            unhealthy=counts[UNHEALTHY],
            services=services,
        )

    def recover(self, service_name: str) -> bool:
        """Manually mark a service healthy. False if the service is unknown."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        if self._breaker.reset(service_name):
            return True

        # Known to the registry but never exercised - nothing to reset, but
        # treat it as a valid target so operators don't get a spurious 404.
        service = self.get(service_name)
        if service is None:
            return False
        self._breaker.register(service_name, service.base_url)
        return True

    def resume_blocked_tasks(self, service_name: str) -> int:
        """Re-queue tasks blocked on a service in every workflow that used it.

        Returns the number of tasks re-queued.
        """
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")
        if self._engine is None:
            return 0

        service = self.get(service_name)
        if service is None:
            return 0

        resumed = 0
        for workflow_id in service.workflow_ids:
            try:
                resumed += self._engine.resume_blocked_tasks(workflow_id, service_name)
            except (WorkflowNotFoundError, TaskNotFoundError):
                # Workflow was deleted since the service was registered.
                continue

        if resumed:
            logger.info(
                "Resumed %d blocked task(s) for service %s",
                resumed,
                service_name,
                extra={
                    "event": "blocked_tasks_resumed",
                    "service_name": service_name,
                    "resumed_tasks": resumed,
                },
            )
        return resumed

    def deregister(self, service_name: str) -> bool:
        """Remove a service from the registry and drop its health record."""
        if not service_name or not service_name.strip():
            raise ValueError("service_name is required")

        removed = bool(self._redis.hdel(REGISTRY_KEY, service_name))
        forgotten = self._breaker.forget(service_name)
        return removed or forgotten
