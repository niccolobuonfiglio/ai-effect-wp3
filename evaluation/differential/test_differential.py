"""Differential tests: the same behavioural checks, run on OLD and on NEW.

Each test states a property that the orchestrator *should* have, using only
the API that both versions share (WorkflowEngine, RedisTaskQueue, Worker, and
the REST API built by OrchestratorAPI).
Run on NEW they should pass. Run on OLD they should fail, which shows the test
detects a real defect of the original code and does not merely exercise
new code. Which source tree is tested is decided by PYTHONPATH (see
run_own_suites.sh).

Where NEW added an optional constructor argument (a lease, a runtime limit),
it is passed only if the constructor accepts it, so OLD runs with its own
defaults, as it does in production.

Environment:
    DIFF_REDIS_URL  a real Redis to use instead of fakeredis (the fan-in race
                    shows up more readily with real network round trips)
    DIFF_TRIALS     repetitions of the fan-in race (default 300)
    DIFF_JITTER_MS  random delay of 0..N ms before every Redis command
                    (default 2). Jitter injection widens the race window the
                    way network latency and scheduling noise do in production;
                    without it the window is too narrow to hit in a test.
    DIFF_OUT        directory for measured rates (differential.json)
"""

import base64
import hashlib
import inspect
import json
import os
import random
import threading
import time
from pathlib import Path

import fakeredis
import pytest
import redis

from fastapi.testclient import TestClient

from api.app import OrchestratorAPI
from models.data_reference import DataReference, Format, Protocol
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import TaskStatus
from services.blueprint_parser import BlueprintParser
from services.control_client import ExecuteResponse, StatusResponse
from services.dockerinfo_parser import DockerInfoParser, ServiceEndpoint
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.worker import Worker
from services.workflow_engine import WorkflowEngine

TRIALS = int(os.environ.get("DIFF_TRIALS", "300"))
JITTER = float(os.environ.get("DIFF_JITTER_MS", "2")) / 1000
HANG = 5.0  # seconds after which a call is considered to hang


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def accepts(cls, name: str) -> bool:
    return name in inspect.signature(cls.__init__).parameters


def optional(cls, **kwargs) -> dict:
    """Keep only the keyword arguments this version's constructor knows."""
    return {k: v for k, v in kwargs.items() if accepts(cls, k)}


class _Jitter:
    """Mixin: sleep a random 0..JITTER seconds before each Redis command."""

    def execute_command(self, *args, **kwargs):
        if JITTER:
            time.sleep(random.uniform(0, JITTER))
        return super().execute_command(*args, **kwargs)


class _JitterRedis(_Jitter, redis.Redis):
    pass


class _JitterFakeRedis(_Jitter, fakeredis.FakeRedis):
    pass


def make_redis(jitter: bool = False):
    url = os.environ.get("DIFF_REDIS_URL")
    if url:
        cls = _JitterRedis if jitter else redis.Redis
        client = cls.from_url(url)
    else:
        client = (_JitterFakeRedis if jitter else fakeredis.FakeRedis)()
    client.flushdb()
    return client


def make_engine(client, **kwargs) -> WorkflowEngine:
    return WorkflowEngine(
        RedisStateStore(client),
        RedisTaskQueue(client),
        client,
        **optional(WorkflowEngine, **kwargs),
    )


def graph_node(name: str) -> GraphNode:
    node = Node(
        container_name=name,
        proto_uri=f"{name}.proto",
        image=f"{name}:latest",
        node_type="microservice",
        operation_signature_list=[
            OperationSignatureList(
                operation_signature=OperationSignature(
                    operation_name="Process",
                    input_message_name="Input",
                    output_message_name="Output",
                )
            )
        ],
    )
    return GraphNode(node=node, operation=node.operation_signature_list[0])


def fan_in_graph() -> ExecutionGraph:
    """A -> C <- B."""
    graph = ExecutionGraph()
    a, b, c = graph_node("service-a"), graph_node("service-b"), graph_node("service-c")
    a.next_nodes.append(c)
    b.next_nodes.append(c)
    c.dependencies.extend([a, b])
    for n in (a, b, c):
        graph.add_node(n)
    graph.start_nodes.extend([a, b])
    return graph


def single_graph() -> ExecutionGraph:
    graph = ExecutionGraph()
    a = graph_node("service-a")
    graph.add_node(a)
    graph.start_nodes.append(a)
    return graph


def ref(name: str) -> DataReference:
    return DataReference(protocol=Protocol.S3, uri=f"s3://bucket/{name}.json", format=Format.JSON)


def in_thread(fn, timeout: float = HANG):
    """Run ``fn`` in a daemon thread; return (finished, result or exception)."""
    box: dict = {}

    def target():
        try:
            box["value"] = fn()
        except Exception as exc:  # reported to the caller
            box["value"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    return (not t.is_alive()), box.get("value")


def record(name: str, value) -> None:
    out = os.environ.get("DIFF_OUT")
    if not out:
        return
    path = Path(out) / "differential.json"
    data = json.loads(path.read_text()) if path.exists() else {}
    data[name] = value
    path.write_text(json.dumps(data, indent=2))


# ----------------------------------------------------------------------
# RQ4: concurrency
# ----------------------------------------------------------------------
def test_fan_in_race_loses_no_input_and_queues_join_once():
    """Two predecessors finishing together: the join gets both inputs, once.

    Repeated TRIALS times; the fraction of trials that violate the property is
    recorded as the empirical failure rate of the race.
    """
    lost, duplicated, errors = 0, 0, 0
    for i in range(TRIALS):
        client = make_redis(jitter=True)
        engine = make_engine(client)
        wf = f"wf-{i}"
        engine.initialize_workflow(wf, fan_in_graph())
        engine.start_workflow(wf)
        a = engine.claim_task(wf, timeout=1)
        b = engine.claim_task(wf, timeout=1)

        barrier = threading.Barrier(2)

        def finish(task, name):
            barrier.wait(timeout=5)
            engine.complete_task(wf, task.task_id, output_refs=[ref(name)])

        threads = [
            threading.Thread(target=finish, args=(a, "a")),
            threading.Thread(target=finish, args=(b, "b")),
        ]
        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join(10)
        except Exception:
            errors += 1
            continue

        queued = RedisTaskQueue(client).queue_length(wf)
        if queued != 1:
            duplicated += 1
        join = engine.claim_task(wf, timeout=1)
        uris = {r.uri for r in (join.input_refs or [])} if join else set()
        if uris != {"s3://bucket/a.json", "s3://bucket/b.json"}:
            lost += 1

    record(
        "fan_in_race",
        {"trials": TRIALS, "lost_inputs": lost, "join_queued_not_once": duplicated, "errors": errors,
         "jitter_ms": JITTER * 1000,
         "redis": "real" if os.environ.get("DIFF_REDIS_URL") else "fakeredis"},
    )
    assert (lost, duplicated, errors) == (0, 0, 0), (
        f"{lost} lost-input and {duplicated} double-queue trials out of {TRIALS}"
    )


# ----------------------------------------------------------------------
# RQ1/RQ4: liveness
# ----------------------------------------------------------------------
def test_claim_on_empty_queue_does_not_block_the_worker():
    """The daemon polls with timeout=0; an empty queue must return at once.

    OLD issued BRPOP with timeout 0 ("wait forever"), so an idle worker hung
    on the queue of an already finished workflow (RUN_ANALYSIS §8, fix 5).
    """
    engine = make_engine(make_redis())
    engine.initialize_workflow("wf-1", single_graph())

    finished, value = in_thread(lambda: engine.claim_task("wf-1", timeout=0))

    record("empty_queue_claim_returns", finished)
    assert finished, f"claim_task(timeout=0) on an empty queue still blocked after {HANG}s"
    assert value is None


def test_task_of_a_dead_worker_is_picked_up_again():
    """A claimed task whose worker dies must not stay 'running' forever.

    The dying worker is simulated by claiming the task and never finishing
    it. Another worker's regular loop must be able to get it back.
    """
    client = make_redis()
    engine = make_engine(client, lease_seconds=0.05)
    engine.initialize_workflow("wf-1", single_graph())
    engine.start_workflow("wf-1")
    task = engine.claim_task("wf-1", timeout=1)
    time.sleep(0.2)

    # What a surviving worker does on each loop iteration, if the version has it.
    release = getattr(engine, "release_due_tasks", None)
    if release:
        release("wf-1")
    again = engine.claim_task("wf-1", timeout=1)

    record("orphaned_task_reclaimed", again is not None)
    assert again is not None, "the task stays 'running' with nobody working on it"
    assert again.task_id == task.task_id


class _StuckService:
    """A control client whose service accepts the task and never finishes it."""

    def execute(self, **kwargs):
        return ExecuteResponse(status="running", task_id="svc-1")

    def get_status(self, base_url, task_id):
        return StatusResponse(status="running")

    def get_output(self, base_url, task_id):  # pragma: no cover - never reached
        raise AssertionError("no output expected")


def test_service_that_never_finishes_does_not_hold_a_worker_forever():
    """A service stuck on 'running' must eventually fail the task."""
    client = make_redis()
    engine = make_engine(client)
    engine.initialize_workflow("wf-1", single_graph())
    engine.start_workflow("wf-1")
    worker = Worker(
        engine,
        _StuckService(),
        {"service-a": ServiceEndpoint(address="service-a", port=8080)},
        poll_interval=0.05,
        **optional(Worker, task_max_runtime=1),
    )

    finished, _ = in_thread(lambda: worker.process_task("wf-1", timeout=1))

    record("stuck_service_bounded", finished)
    assert finished, f"worker still polling a stuck service after {HANG}s"
    tasks = engine.get_all_tasks("wf-1")
    assert tasks[0].status == TaskStatus.FAILED


# ----------------------------------------------------------------------
# RQ8: data-exchange contracts (PROTOCOL.md, Amendment 1)
# ----------------------------------------------------------------------
def chain_graph() -> ExecutionGraph:
    """A -> B."""
    graph = ExecutionGraph()
    a, b = graph_node("service-a"), graph_node("service-b")
    a.next_nodes.append(b)
    b.dependencies.append(a)
    graph.add_node(a)
    graph.add_node(b)
    graph.start_nodes.append(a)
    return graph


def blueprint(out_a: str = "Msg", in_b: str = "Msg") -> dict:
    """Two connected services; the message types match unless told otherwise."""
    def node(name, inp, out, targets):
        return {
            "container_name": name,
            "proto_uri": f"{name}.proto",
            "image": f"{name}:latest",
            "node_type": "MLModel",
            "operation_signature_list": [{
                "operation_signature": {
                    "operation_name": "Run",
                    "input_message_name": inp,
                    "output_message_name": out,
                },
                "connected_to": [
                    {"container_name": t, "operation_signature": {"operation_name": "Run"}}
                    for t in targets
                ],
            }],
        }
    return {
        "name": "differential", "pipeline_id": "diff", "creation_date": "2026-09-26",
        "type": "pipeline-topology/v2", "version": "2.0",
        "nodes": [node("svc-a", "Start", out_a, ["svc-b"]), node("svc-b", in_b, "End", [])],
    }


DOCKERINFO = {"docker_info_list": [
    {"container_name": "svc-a", "ip_address": "svc-a", "port": "8080"},
    {"container_name": "svc-b", "ip_address": "svc-b", "port": "8080"},
]}


def api_client(monkeypatch, client, **kwargs) -> TestClient:
    """This version's REST API on ``client``, with the API key switched off."""
    monkeypatch.delenv("ORCHESTRATOR_API_KEY", raising=False)
    api = OrchestratorAPI(
        make_engine(client), BlueprintParser(), DockerInfoParser(), client,
        **optional(OrchestratorAPI, **kwargs),
    )
    return TestClient(api.create_app())


def keys(client, pattern: str) -> list:
    return list(client.scan_iter(match=pattern))


def test_fan_in_inputs_name_their_producer():
    """A task with two predecessors can tell its inputs apart."""
    engine = make_engine(make_redis())
    engine.initialize_workflow("wf-1", fan_in_graph())
    engine.start_workflow("wf-1")
    for _ in range(2):
        task = engine.claim_task("wf-1", timeout=1)
        producer = task.node_key.split(":")[0]
        engine.complete_task("wf-1", task.task_id, output_refs=[ref(producer)])

    join = engine.claim_task("wf-1", timeout=1)
    labels = {r.uri: (r.metadata or {}).get("source_node") for r in join.input_refs}

    record("fan_in_inputs_labelled", all(labels.values()))
    assert labels == {
        "s3://bucket/service-a.json": "service-a:Process",
        "s3://bucket/service-b.json": "service-b:Process",
    }


def test_services_key_is_gone_once_the_workflow_completes():
    """A workflow's service credentials do not outlive it (T3.3 German 50)."""
    client = make_redis()
    engine = make_engine(client)
    engine.initialize_workflow("wf-1", chain_graph())
    client.set("services_key:wf-1", "secret")
    engine.start_workflow("wf-1")
    for _ in range(2):
        task = engine.claim_task("wf-1", timeout=1)
        engine.complete_task("wf-1", task.task_id)

    left = bool(client.exists("services_key:wf-1"))
    record("services_key_left_after_completion", left)
    assert not left, "the services key is still in Redis after the workflow completed"


@pytest.mark.httpx_mock(assert_all_responses_were_requested=False)
def test_submission_with_refused_credentials_is_rejected(monkeypatch, httpx_mock):
    """Services that refuse the key: rejected at once, nothing stored."""
    httpx_mock.add_response(status_code=401, is_reusable=True)
    client = make_redis()
    api = api_client(monkeypatch, client, service_auth_check=True)

    response = api.post("/workflows", json={
        "blueprint": blueprint(), "dockerinfo": DOCKERINFO, "services_api_key": "wrong",
    })

    stored = keys(client, "services_key:*") + keys(client, "endpoints:*")
    record("refused_credentials_rejected", response.status_code == 400)
    assert response.status_code == 400, "accepted a workflow its services will refuse"
    assert stored == []


def test_mismatching_message_types_are_reported(monkeypatch):
    """A connection whose message types differ is reported at submission."""
    client = make_redis()
    api = api_client(monkeypatch, client, service_auth_check=False)

    response = api.post("/workflows", json={
        "blueprint": blueprint(out_a="Forecast", in_b="GridModel"), "dockerinfo": DOCKERINFO,
    })

    reported = response.status_code == 400 or bool(response.json().get("warnings"))
    record("type_mismatch_reported", reported)
    assert reported, "a Forecast -> GridModel connection went unnoticed"


def test_inline_input_with_wrong_checksum_is_rejected(monkeypatch):
    """Inline data that does not match its own checksum never enters a workflow."""
    client = make_redis()
    api = api_client(monkeypatch, client, service_auth_check=False)
    data = b'{"num_records": 10}'
    wrong = "sha256:" + hashlib.sha256(b"something else").hexdigest()

    response = api.post("/workflows", json={
        "blueprint": blueprint(), "dockerinfo": DOCKERINFO,
        "inputs": [{"protocol": "inline", "uri": base64.b64encode(data).decode(),
                    "format": "json", "checksum": wrong}],
    })

    record("corrupt_inline_rejected", response.status_code == 400)
    assert response.status_code == 400, "a corrupted inline input was accepted"
