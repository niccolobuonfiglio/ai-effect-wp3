"""Unit tests for the service registry."""

import json

import fakeredis
import pytest

from services.circuit_breaker import DEGRADED, HEALTHY, UNHEALTHY, CircuitBreaker
from services.dockerinfo_parser import ServiceEndpoint
from services.service_registry import (
    REGISTRY_KEY,
    RegisteredService,
    ServiceNotFoundError,
    ServiceRegistry,
)

SERVICE = "databroker"
ENDPOINT = ServiceEndpoint(address="databroker", port=8080)
BASE_URL = "http://databroker:8080"


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def breaker(redis_client):
    return CircuitBreaker(redis_client, failure_threshold=3, reset_timeout=60.0)


@pytest.fixture
def registry(redis_client, breaker):
    return ServiceRegistry(redis_client, breaker)


class TestConstruction:
    def test_requires_redis_client(self, breaker):
        with pytest.raises(ValueError, match="redis_client is required"):
            ServiceRegistry(None, breaker)

    def test_requires_breaker(self, redis_client):
        with pytest.raises(ValueError, match="breaker is required"):
            ServiceRegistry(redis_client, None)


class TestRegister:
    def test_register_returns_record(self, registry):
        service = registry.register(SERVICE, ENDPOINT)

        assert isinstance(service, RegisteredService)
        assert service.service_name == SERVICE
        assert service.address == "databroker"
        assert service.port == 8080
        assert service.base_url == BASE_URL
        assert service.first_seen
        assert service.last_seen

    def test_register_persists(self, registry):
        registry.register(SERVICE, ENDPOINT)

        assert registry.get(SERVICE).base_url == BASE_URL
        assert registry.is_registered(SERVICE) is True

    def test_register_mirrors_into_breaker(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        health = breaker.get_health(SERVICE)

        assert health is not None
        assert health.status == HEALTHY
        assert health.base_url == BASE_URL

    def test_first_seen_is_stable_across_re_registration(self, registry):
        first = registry.register(SERVICE, ENDPOINT)
        again = registry.register(SERVICE, ENDPOINT)

        assert again.first_seen == first.first_seen

    def test_re_registration_updates_endpoint(self, registry):
        registry.register(SERVICE, ENDPOINT)
        registry.register(SERVICE, ServiceEndpoint(address="10.0.0.5", port=9090))
        service = registry.get(SERVICE)

        assert service.address == "10.0.0.5"
        assert service.port == 9090
        assert service.base_url == "http://10.0.0.5:9090"

    def test_register_rejects_blank_name(self, registry):
        with pytest.raises(ValueError, match="service_name is required"):
            registry.register("  ", ENDPOINT)

    def test_register_rejects_missing_endpoint(self, registry):
        with pytest.raises(ValueError, match="endpoint is required"):
            registry.register(SERVICE, None)

    def test_register_does_not_clear_existing_failures(self, registry, breaker):
        breaker.record_failure(SERVICE, BASE_URL, "boom")
        registry.register(SERVICE, ENDPOINT)

        assert breaker.get_health(SERVICE).failure_count == 1


class TestWorkflowTracking:
    def test_records_workflow_id(self, registry):
        service = registry.register(SERVICE, ENDPOINT, workflow_id="wf-1")
        assert service.workflow_ids == ["wf-1"]

    def test_accumulates_distinct_workflows(self, registry):
        registry.register(SERVICE, ENDPOINT, workflow_id="wf-1")
        registry.register(SERVICE, ENDPOINT, workflow_id="wf-2")

        assert registry.get(SERVICE).workflow_ids == ["wf-1", "wf-2"]

    def test_does_not_duplicate_workflow_id(self, registry):
        for _ in range(3):
            registry.register(SERVICE, ENDPOINT, workflow_id="wf-1")

        assert registry.get(SERVICE).workflow_ids == ["wf-1"]

    def test_workflow_list_is_capped(self, registry):
        for i in range(ServiceRegistry.MAX_WORKFLOW_IDS + 10):
            registry.register(SERVICE, ENDPOINT, workflow_id=f"wf-{i}")

        ids = registry.get(SERVICE).workflow_ids
        assert len(ids) == ServiceRegistry.MAX_WORKFLOW_IDS
        assert ids[-1] == f"wf-{ServiceRegistry.MAX_WORKFLOW_IDS + 9}"

    def test_register_without_workflow_id(self, registry):
        assert registry.register(SERVICE, ENDPOINT).workflow_ids == []


class TestRegisterAll:
    def test_registers_every_endpoint(self, registry):
        endpoints = {
            "svc-b": ServiceEndpoint(address="svc-b", port=8080),
            "svc-a": ServiceEndpoint(address="svc-a", port=8081),
        }
        registered = registry.register_all(endpoints, workflow_id="wf-1")

        assert [s.service_name for s in registered] == ["svc-a", "svc-b"]
        assert registry.is_registered("svc-a")
        assert registry.is_registered("svc-b")

    def test_empty_mapping_is_noop(self, registry):
        assert registry.register_all({}) == []
        assert registry.list_services() == []

    def test_rejects_none(self, registry):
        with pytest.raises(ValueError, match="endpoints is required"):
            registry.register_all(None)


class TestLookup:
    def test_get_unknown_returns_none(self, registry):
        assert registry.get("ghost") is None

    def test_get_or_raise_unknown(self, registry):
        with pytest.raises(ServiceNotFoundError) as exc:
            registry.get_or_raise("ghost")
        assert exc.value.service_name == "ghost"

    def test_get_or_raise_known(self, registry):
        registry.register(SERVICE, ENDPOINT)
        assert registry.get_or_raise(SERVICE).service_name == SERVICE

    def test_base_url_for(self, registry):
        registry.register(SERVICE, ENDPOINT)
        assert registry.base_url_for(SERVICE) == BASE_URL

    def test_base_url_for_unknown_raises(self, registry):
        with pytest.raises(ServiceNotFoundError):
            registry.base_url_for("ghost")

    def test_is_registered_false_for_unknown(self, registry):
        assert registry.is_registered("ghost") is False

    def test_list_is_sorted(self, registry):
        for name in ("zebra", "alpha", "mid"):
            registry.register(name, ServiceEndpoint(address=name, port=8080))

        assert [s.service_name for s in registry.list_services()] == [
            "alpha",
            "mid",
            "zebra",
        ]

    def test_list_empty(self, registry):
        assert registry.list_services() == []

    def test_corrupt_entry_skipped_in_list(self, registry, redis_client):
        registry.register(SERVICE, ENDPOINT)
        redis_client.hset(REGISTRY_KEY, "broken", "{not json")

        assert [s.service_name for s in registry.list_services()] == [SERVICE]

    def test_rejects_blank_name(self, registry):
        with pytest.raises(ValueError, match="service_name is required"):
            registry.get("")


class TestHealth:
    def test_health_for_registered_but_unexercised_service(self, registry):
        registry.register(SERVICE, ENDPOINT)
        health = registry.get_health(SERVICE)

        assert health is not None
        assert health.status == HEALTHY

    def test_health_reflects_failures(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        breaker.record_failure(SERVICE, BASE_URL, "timeout")

        assert registry.get_health(SERVICE).status == DEGRADED

    def test_health_reflects_open_circuit(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        for _ in range(3):
            breaker.record_failure(SERVICE, BASE_URL, "dead")

        health = registry.get_health(SERVICE)
        assert health.status == UNHEALTHY
        assert health.last_error == "dead"

    def test_health_unknown_service(self, registry):
        assert registry.get_health("ghost") is None

    def test_health_for_service_only_known_to_breaker(self, registry, breaker):
        """A worker may exercise a service this replica never registered."""
        breaker.record_failure("stray", "http://stray:8080", "boom")

        assert registry.get_health("stray") is not None


class TestSummary:
    def test_empty_summary_is_healthy(self, registry):
        summary = registry.get_summary()

        assert summary.overall_status == HEALTHY
        assert summary.total == 0
        assert summary.services == []

    def test_counts_by_status(self, registry, breaker):
        registry.register("ok", ServiceEndpoint(address="ok", port=8080))
        registry.register("warn", ServiceEndpoint(address="warn", port=8080))
        registry.register("bad", ServiceEndpoint(address="bad", port=8080))

        breaker.record_failure("warn", "http://warn:8080", "flaky")
        for _ in range(3):
            breaker.record_failure("bad", "http://bad:8080", "dead")

        summary = registry.get_summary()
        assert summary.total == 3
        assert summary.healthy == 1
        assert summary.degraded == 1
        assert summary.unhealthy == 1

    def test_overall_unhealthy_wins_over_degraded(self, registry, breaker):
        registry.register("warn", ServiceEndpoint(address="warn", port=8080))
        registry.register("bad", ServiceEndpoint(address="bad", port=8080))
        breaker.record_failure("warn", "http://warn:8080", "flaky")
        for _ in range(3):
            breaker.record_failure("bad", "http://bad:8080", "dead")

        assert registry.get_summary().overall_status == UNHEALTHY

    def test_overall_degraded_when_no_outage(self, registry, breaker):
        registry.register("ok", ServiceEndpoint(address="ok", port=8080))
        registry.register("warn", ServiceEndpoint(address="warn", port=8080))
        breaker.record_failure("warn", "http://warn:8080", "flaky")

        assert registry.get_summary().overall_status == DEGRADED

    def test_overall_healthy_when_all_fine(self, registry):
        registry.register("a", ServiceEndpoint(address="a", port=8080))
        registry.register("b", ServiceEndpoint(address="b", port=8080))

        assert registry.get_summary().overall_status == HEALTHY

    def test_summary_sorted_by_name(self, registry):
        for name in ("zebra", "alpha"):
            registry.register(name, ServiceEndpoint(address=name, port=8080))

        names = [h.service_name for h in registry.get_summary().services]
        assert names == ["alpha", "zebra"]

    def test_summary_unions_both_stores(self, registry, breaker):
        registry.register("registered", ServiceEndpoint(address="r", port=8080))
        breaker.record_failure("exercised", "http://e:8080", "boom")

        names = [h.service_name for h in registry.get_summary().services]
        assert names == ["exercised", "registered"]
        assert registry.get_summary().total == 2

    def test_no_duplicates_when_in_both_stores(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        breaker.record_failure(SERVICE, BASE_URL, "boom")

        summary = registry.get_summary()
        assert summary.total == 1
        assert len(summary.services) == 1


class TestRecover:
    def test_recover_resets_open_circuit(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        for _ in range(3):
            breaker.record_failure(SERVICE, BASE_URL, "dead")
        assert breaker.allow(SERVICE) is False

        assert registry.recover(SERVICE) is True

        assert breaker.allow(SERVICE) is True
        assert registry.get_health(SERVICE).status == HEALTHY

    def test_recover_registered_but_unexercised_service(self, registry):
        """No breaker record yet, but the service is known - not a 404."""
        registry.register(SERVICE, ENDPOINT)
        assert registry.recover(SERVICE) is True

    def test_recover_unknown_returns_false(self, registry):
        assert registry.recover("ghost") is False

    def test_recover_sets_manual_flag(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        for _ in range(3):
            breaker.record_failure(SERVICE, BASE_URL, "dead")
        registry.recover(SERVICE)

        assert registry.get_health(SERVICE).manually_recovered_at is not None

    def test_service_can_degrade_again_after_recovery(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)
        for _ in range(3):
            breaker.record_failure(SERVICE, BASE_URL, "dead")
        registry.recover(SERVICE)
        for _ in range(3):
            breaker.record_failure(SERVICE, BASE_URL, "dead again")

        assert registry.get_health(SERVICE).status == UNHEALTHY

    def test_rejects_blank_name(self, registry):
        with pytest.raises(ValueError, match="service_name is required"):
            registry.recover("")


class TestDeregister:
    def test_removes_from_both_stores(self, registry, breaker):
        registry.register(SERVICE, ENDPOINT)

        assert registry.deregister(SERVICE) is True
        assert registry.get(SERVICE) is None
        assert breaker.get_health(SERVICE) is None

    def test_unknown_returns_false(self, registry):
        assert registry.deregister("ghost") is False

    def test_removes_breaker_only_record(self, registry, breaker):
        breaker.record_failure("stray", "http://stray:8080", "boom")
        assert registry.deregister("stray") is True

    def test_rejects_blank_name(self, registry):
        with pytest.raises(ValueError, match="service_name is required"):
            registry.deregister("   ")


class TestSharedState:
    def test_registries_share_redis(self, redis_client):
        breaker_a = CircuitBreaker(redis_client)
        breaker_b = CircuitBreaker(redis_client)
        api = ServiceRegistry(redis_client, breaker_a)
        worker = ServiceRegistry(redis_client, breaker_b)

        api.register(SERVICE, ENDPOINT, workflow_id="wf-1")

        assert worker.base_url_for(SERVICE) == BASE_URL

    def test_worker_failures_visible_to_api(self, redis_client):
        breaker_worker = CircuitBreaker(redis_client, failure_threshold=2)
        breaker_api = CircuitBreaker(redis_client, failure_threshold=2)
        worker = ServiceRegistry(redis_client, breaker_worker)
        api = ServiceRegistry(redis_client, breaker_api)

        worker.register(SERVICE, ENDPOINT)
        breaker_worker.record_failure(SERVICE, BASE_URL, "boom")
        breaker_worker.record_failure(SERVICE, BASE_URL, "boom")

        assert api.get_summary().overall_status == UNHEALTHY

    def test_api_recovery_unblocks_worker(self, redis_client):
        breaker_worker = CircuitBreaker(redis_client, failure_threshold=1)
        breaker_api = CircuitBreaker(redis_client, failure_threshold=1)
        worker = ServiceRegistry(redis_client, breaker_worker)
        api = ServiceRegistry(redis_client, breaker_api)

        worker.register(SERVICE, ENDPOINT)
        breaker_worker.record_failure(SERVICE, BASE_URL, "down")
        assert breaker_worker.allow(SERVICE) is False

        api.recover(SERVICE)

        assert breaker_worker.allow(SERVICE) is True


class TestPersistence:
    def test_record_shape_in_redis(self, registry, redis_client):
        registry.register(SERVICE, ENDPOINT, workflow_id="wf-1")
        raw = json.loads(redis_client.hget(REGISTRY_KEY, SERVICE))

        assert raw["service_name"] == SERVICE
        assert raw["port"] == 8080
        assert raw["workflow_ids"] == ["wf-1"]

    def test_to_dict_copies_workflow_ids(self, registry):
        registry.register(SERVICE, ENDPOINT, workflow_id="wf-1")
        service = registry.get(SERVICE)
        data = service.to_dict()
        data["workflow_ids"].append("mutated")

        assert registry.get(SERVICE).workflow_ids == ["wf-1"]
