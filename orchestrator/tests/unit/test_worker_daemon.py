"""Tests for the worker daemon, the process that runs in production.

The daemon is glue: it finds running workflows, loads their endpoints and
services key from Redis, builds a Worker and loops. These tests cover that glue
against fakeredis, plus one run of a real workflow through the real engine with
only the HTTP client faked.
"""

import json
import logging
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

import worker_daemon
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import TaskStatus, WorkflowStatus
from services.control_client import ControlClientError, ExecuteResponse
from services.secrets import SecretBox
from worker_daemon import WorkerDaemon


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Start every test from default settings."""
    for name in (
        "ORCHESTRATOR_SECRET",
        "SERVICE_TLS_VERIFY",
        "CIRCUIT_FAILURE_THRESHOLD",
        "CIRCUIT_RESET_TIMEOUT",
        "BLOCKED_TASK_RETRY_DELAY",
        "BLOCKED_TASK_RETRY_INITIAL",
        "CIRCUIT_RESET_INITIAL",
        "BLOCKED_TASK_MAX_AGE",
        "TASK_MAX_RUNTIME",
        "LEASE_SECONDS",
        "ALERT_REPEAT_SECONDS",
        "ALLOW_INLINE",
        "MAX_INLINE_BYTES",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def daemon(redis_client):
    return WorkerDaemon(redis_client, poll_interval=0.01)


def chain_graph(*names: str) -> ExecutionGraph:
    """Linear graph over the given container names, operation "Process"."""
    graph = ExecutionGraph()
    previous = None
    for name in names:
        signature = OperationSignatureList(
            operation_signature=OperationSignature(
                operation_name="Process",
                input_message_name="Input",
                output_message_name="Output",
            )
        )
        node = GraphNode(
            node=Node(
                container_name=name,
                proto_uri=f"{name}.proto",
                image=f"{name}:latest",
                node_type="microservice",
                operation_signature_list=[signature],
            ),
            operation=signature,
        )
        graph.add_node(node)
        if previous is None:
            graph.start_nodes.append(node)
        else:
            previous.next_nodes.append(node)
            node.dependencies.append(previous)
        previous = node
    return graph


def store_endpoints(redis_client, workflow_id, endpoints: dict) -> None:
    redis_client.hset(
        f"endpoints:{workflow_id}",
        mapping={name: json.dumps(data) for name, data in endpoints.items()},
    )


# ----------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------
class TestConfiguration:
    def test_defaults(self, daemon):
        assert daemon.verify is True
        assert daemon.blocked_retry_delay == 30.0
        assert daemon.blocked_retry_initial == 2.0
        assert daemon.blocked_max_age == 0.0          # no limit unless configured
        assert daemon.task_max_runtime == 3600.0      # a task may not hang forever
        assert daemon.lease_seconds == 300.0
        assert daemon.breaker._alert_repeat == 900.0
        assert daemon.breaker._initial_reset_timeout == 5.0
        assert daemon.breaker._reset_timeout == 60.0
        assert daemon.secrets.enabled is False
        assert daemon.policy.allow_inline is True

    def test_reads_environment(self, redis_client, monkeypatch):
        monkeypatch.setenv("BLOCKED_TASK_RETRY_DELAY", "5")
        monkeypatch.setenv("ORCHESTRATOR_SECRET", "passphrase")
        monkeypatch.setenv("MAX_INLINE_BYTES", "128")
        monkeypatch.setenv("BLOCKED_TASK_MAX_AGE", "3600")
        monkeypatch.setenv("ALERT_REPEAT_SECONDS", "60")
        monkeypatch.setenv("TASK_MAX_RUNTIME", "120")
        monkeypatch.setenv("LEASE_SECONDS", "45")

        daemon = WorkerDaemon(redis_client)

        assert daemon.blocked_retry_delay == 5.0
        assert daemon.secrets.enabled is True
        assert daemon.policy.max_inline_bytes == 128
        assert daemon.blocked_max_age == 3600.0
        assert daemon.breaker._alert_repeat == 60.0
        assert daemon.task_max_runtime == 120.0
        assert daemon.lease_seconds == 45.0

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("true", True),
            ("TRUE", True),
            ("1", True),
            ("", True),
            ("false", False),
            ("off", False),
            ("0", False),
            ("/etc/ssl/private-ca.pem", "/etc/ssl/private-ca.pem"),
        ],
    )
    def test_tls_verify_parsing(self, raw, expected):
        assert WorkerDaemon._tls_verify(raw) == expected

    def test_tls_setting_reaches_client(self, redis_client, monkeypatch):
        monkeypatch.setenv("SERVICE_TLS_VERIFY", "false")

        daemon = WorkerDaemon(redis_client)

        assert daemon.verify is False
        assert daemon.control_client._verify is False


# ----------------------------------------------------------------------
# endpoints
# ----------------------------------------------------------------------
class TestLoadEndpoints:
    def test_no_endpoints(self, daemon):
        assert daemon.load_endpoints("wf-none") == {}

    def test_loads_http_and_https(self, daemon, redis_client):
        store_endpoints(
            redis_client,
            "wf-1",
            {
                "plain": {"address": "svc-a", "port": 8080, "scheme": "http"},
                "secure": {"address": "svc-b", "port": 8443, "scheme": "https"},
            },
        )

        endpoints = daemon.load_endpoints("wf-1")

        assert endpoints["plain"].base_url == "http://svc-a:8080"
        assert endpoints["secure"].base_url == "https://svc-b:8443"

    def test_endpoint_stored_before_scheme_existed(self, daemon, redis_client):
        """Workflows submitted by an older orchestrator have no scheme field."""
        store_endpoints(redis_client, "wf-old", {"svc": {"address": "svc", "port": 8080}})

        assert daemon.load_endpoints("wf-old")["svc"].base_url == "http://svc:8080"


# ----------------------------------------------------------------------
# workflow discovery
# ----------------------------------------------------------------------
class TestGetRunningWorkflows:
    def test_only_running_workflows(self, daemon, redis_client):
        for wf, status in (
            ("wf-run", "running"),
            ("wf-done", "completed"),
            ("wf-fail", "failed"),
            ("wf-new", "pending"),
        ):
            redis_client.set(f"workflow:{wf}", json.dumps({"status": status}))

        assert daemon.get_running_workflows() == ["wf-run"]

    def test_ignores_sub_keys(self, daemon, redis_client):
        redis_client.set("workflow:wf-1", json.dumps({"status": "running"}))
        redis_client.sadd("workflow:wf-1:tasks", "task-a")

        assert daemon.get_running_workflows() == ["wf-1"]

    def test_skips_corrupt_entries(self, daemon, redis_client):
        redis_client.set("workflow:wf-bad", "not json")
        redis_client.set("workflow:wf-ok", json.dumps({"status": "running"}))

        assert daemon.get_running_workflows() == ["wf-ok"]

    def test_scans_past_one_page(self, daemon, redis_client):
        for i in range(250):
            redis_client.set(f"workflow:wf-{i}", json.dumps({"status": "running"}))

        assert len(daemon.get_running_workflows()) == 250


# ----------------------------------------------------------------------
# services key
# ----------------------------------------------------------------------
class TestLoadServicesKey:
    def test_missing_key(self, daemon):
        assert daemon.load_services_key("wf-1") is None

    def test_plaintext_key(self, daemon, redis_client):
        redis_client.set("services_key:wf-1", "plain-key")

        assert daemon.load_services_key("wf-1") == "plain-key"

    def test_encrypted_key(self, redis_client, monkeypatch):
        monkeypatch.setenv("ORCHESTRATOR_SECRET", "passphrase")
        redis_client.set(
            "services_key:wf-1", SecretBox("passphrase").encrypt("real-key")
        )

        assert WorkerDaemon(redis_client).load_services_key("wf-1") == "real-key"

    def test_unreadable_key_is_logged_not_raised(self, redis_client, monkeypatch, caplog):
        monkeypatch.setenv("ORCHESTRATOR_SECRET", "new-passphrase")
        redis_client.set(
            "services_key:wf-1", SecretBox("old-passphrase").encrypt("real-key")
        )
        daemon = WorkerDaemon(redis_client)

        with caplog.at_level(logging.ERROR, logger="worker_daemon"):
            assert daemon.load_services_key("wf-1") is None

        assert any(
            getattr(r, "event", None) == "services_key_unreadable" for r in caplog.records
        )


# ----------------------------------------------------------------------
# processing one workflow
# ----------------------------------------------------------------------
class TestProcessWorkflow:
    def test_skips_workflow_without_endpoints(self, daemon):
        with patch.object(worker_daemon, "Worker") as worker_cls:
            assert daemon.process_workflow("wf-1") is False
        worker_cls.assert_not_called()

    def test_builds_worker_with_daemon_settings(self, daemon, redis_client):
        store_endpoints(redis_client, "wf-1", {"svc": {"address": "svc", "port": 8080}})

        with patch.object(worker_daemon, "Worker") as worker_cls:
            worker_cls.return_value.process_task.return_value = True
            assert daemon.process_workflow("wf-1") is True

        kwargs = worker_cls.call_args.kwargs
        assert kwargs["breaker"] is daemon.breaker
        assert kwargs["policy"] is daemon.policy
        assert kwargs["blocked_retry_delay"] == daemon.blocked_retry_delay
        assert kwargs["blocked_retry_initial"] == daemon.blocked_retry_initial
        assert kwargs["blocked_max_age"] == daemon.blocked_max_age
        assert worker_cls.call_args.args[1] is daemon.control_client
        worker_cls.return_value.process_task.assert_called_once_with("wf-1", timeout=0)

    def test_uses_services_key_when_present(self, daemon, redis_client):
        store_endpoints(redis_client, "wf-1", {"svc": {"address": "svc", "port": 8080}})
        redis_client.set("services_key:wf-1", "svc-key")

        with patch.object(worker_daemon, "Worker") as worker_cls:
            daemon.process_workflow("wf-1")

        client = worker_cls.call_args.args[1]
        assert client is not daemon.control_client
        assert client._headers == {"Authorization": "Bearer svc-key"}


# ----------------------------------------------------------------------
# main loop
# ----------------------------------------------------------------------
class TestRunLoop:
    def stop_after(self, daemon, sleeps: int):
        """time.sleep replacement that stops the daemon after N sleeps."""
        calls = {"n": 0}

        def fake_sleep(_seconds):
            calls["n"] += 1
            if calls["n"] >= sleeps:
                daemon.stop()

        return fake_sleep, calls

    def test_sleeps_when_idle(self, daemon):
        fake_sleep, calls = self.stop_after(daemon, 1)
        with patch.object(worker_daemon.time, "sleep", fake_sleep):
            daemon.run()

        assert calls["n"] == 1

    def test_error_in_one_workflow_does_not_stop_the_loop(self, daemon, caplog):
        fake_sleep, _ = self.stop_after(daemon, 1)
        with patch.object(daemon, "get_running_workflows", return_value=["wf-a", "wf-b"]), \
             patch.object(daemon, "process_workflow", side_effect=[RuntimeError("boom"), False]) as process, \
             patch.object(worker_daemon.time, "sleep", fake_sleep), \
             caplog.at_level(logging.ERROR, logger="worker_daemon"):
            daemon.run()

        assert process.call_count == 2
        assert any("Error processing workflow wf-a: boom" in r.getMessage() for r in caplog.records)

    def test_error_in_discovery_is_survived(self, daemon, caplog):
        fake_sleep, calls = self.stop_after(daemon, 2)
        with patch.object(daemon, "get_running_workflows", side_effect=[RuntimeError("redis down"), []]), \
             patch.object(worker_daemon.time, "sleep", fake_sleep), \
             caplog.at_level(logging.ERROR, logger="worker_daemon"):
            daemon.run()

        assert calls["n"] == 2
        assert any("Error in daemon loop: redis down" in r.getMessage() for r in caplog.records)

    def test_no_sleep_while_work_is_found(self, daemon):
        results = iter([True, True, False])
        fake_sleep, calls = self.stop_after(daemon, 1)
        with patch.object(daemon, "get_running_workflows", return_value=["wf-a"]), \
             patch.object(daemon, "process_workflow", side_effect=lambda _wf: next(results)), \
             patch.object(worker_daemon.time, "sleep", fake_sleep):
            daemon.run()

        assert calls["n"] == 1   # only after the third, empty, iteration


class TestMain:
    def test_each_replica_writes_its_own_files(self, monkeypatch):
        """Replicas share one logs volume, so file names carry the hostname."""
        import redis

        monkeypatch.setenv("HOSTNAME", "abc123")
        client = MagicMock()
        client.ping.side_effect = redis.ConnectionError("refused")
        with patch.object(worker_daemon, "configure_logging") as configure, \
             patch.object(worker_daemon.redis.Redis, "from_url", return_value=client):
            worker_daemon.main()

        kwargs = configure.call_args.kwargs
        assert kwargs["log_file"] == "worker-abc123.log"
        assert kwargs["alerts_file"] == "alerts-worker-abc123.jsonl"

    def test_returns_1_when_redis_unreachable(self):
        import redis

        client = MagicMock()
        client.ping.side_effect = redis.ConnectionError("refused")
        with patch.object(worker_daemon, "configure_logging"), \
             patch.object(worker_daemon.redis.Redis, "from_url", return_value=client):
            assert worker_daemon.main() == 1


# ----------------------------------------------------------------------
# a real workflow through the daemon
# ----------------------------------------------------------------------
class TestDaemonDrivesRealWorkflow:
    """Real engine, queue, state store and breaker on fakeredis; only HTTP is faked."""

    def submit(self, daemon, redis_client, workflow_id="wf-real"):
        daemon.engine.initialize_workflow(workflow_id, chain_graph("svc-a", "svc-b"))
        store_endpoints(
            redis_client,
            workflow_id,
            {
                "svc-a": {"address": "svc-a", "port": 8080},
                "svc-b": {"address": "svc-b", "port": 8080},
            },
        )
        daemon.engine.start_workflow(workflow_id)
        return workflow_id

    def drain(self, daemon, workflow_id, max_steps=10):
        for _ in range(max_steps):
            if not daemon.process_workflow(workflow_id):
                break

    def test_workflow_completes(self, daemon, redis_client):
        daemon.control_client = MagicMock()
        daemon.control_client.execute.return_value = ExecuteResponse(status="complete")
        wf = self.submit(daemon, redis_client)

        assert daemon.get_running_workflows() == [wf]
        self.drain(daemon, wf)

        assert daemon.engine.get_workflow_status(wf).status == WorkflowStatus.COMPLETED
        assert daemon.get_running_workflows() == []
        called = [c.kwargs["base_url"] for c in daemon.control_client.execute.call_args_list]
        assert called == ["http://svc-a:8080", "http://svc-b:8080"]

    def test_unreachable_service_blocks_instead_of_failing(self, redis_client, monkeypatch):
        # Same threshold as the worker in docker-compose.yml: open on first failure.
        monkeypatch.setenv("CIRCUIT_FAILURE_THRESHOLD", "1")
        daemon = WorkerDaemon(redis_client, poll_interval=0.01)
        daemon.control_client = MagicMock()

        def execute(base_url, **_kwargs):
            if "svc-b" in base_url:
                raise ControlClientError("Connection failed: refused", transient=True)
            return ExecuteResponse(status="complete")

        daemon.control_client.execute.side_effect = execute
        wf = self.submit(daemon, redis_client)
        self.drain(daemon, wf)

        tasks = {t.node_key: t.status for t in daemon.engine.get_all_tasks(wf)}
        assert tasks == {
            "svc-a:Process": TaskStatus.COMPLETED,
            "svc-b:Process": TaskStatus.BLOCKED,
        }
        assert daemon.engine.get_workflow_status(wf).status == WorkflowStatus.RUNNING
        assert daemon.breaker.get_health("svc-b").status == "UNHEALTHY"

    def test_default_threshold_reports_degraded_first(self, daemon, redis_client):
        """With the code default (3) one failure is DEGRADED, not UNHEALTHY."""
        daemon.control_client = MagicMock()
        daemon.control_client.execute.side_effect = ControlClientError(
            "Connection failed: refused", transient=True
        )
        wf = self.submit(daemon, redis_client)
        self.drain(daemon, wf)

        health = daemon.breaker.get_health("svc-a")
        assert health.status == "DEGRADED"
        assert health.failure_count == 1
