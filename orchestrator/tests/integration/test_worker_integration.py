"""Integration tests for Worker with real services."""

import sys
from pathlib import Path

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from models.data_reference import DataReference, Format, Protocol
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from services.control_client import ControlClient
from services.dockerinfo_parser import ServiceEndpoint
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.worker import Worker
from services.workflow_engine import WorkflowEngine

# Import test service runner
sys.path.insert(0, str(Path(__file__).parent.parent / "fixtures"))
from test_service import ControlServiceRunner


@pytest.fixture(scope="module")
def test_service():
    """Start test service for module."""
    runner = ControlServiceRunner(port=18082)
    runner.start()
    yield runner
    runner.stop()


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as container:
        yield container


@pytest.fixture
def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=False,
    )
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def engine(redis_client):
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    return WorkflowEngine(state_store, task_queue, redis_client)


@pytest.fixture
def control_client():
    return ControlClient(timeout=10.0)


@pytest.fixture
def endpoints(test_service):
    """Endpoints pointing to test service."""
    # Parse host:port from base_url
    url = test_service.base_url
    # http://127.0.0.1:18082 -> 127.0.0.1, 18082
    host_port = url.replace("http://", "")
    host, port = host_port.split(":")

    return {
        "service-a": ServiceEndpoint(address=host, port=int(port)),
        "service-b": ServiceEndpoint(address=host, port=int(port)),
        "service-c": ServiceEndpoint(address=host, port=int(port)),
    }


@pytest.fixture
def worker(engine, control_client, endpoints):
    return Worker(engine, control_client, endpoints, poll_interval=0.1)


@pytest.fixture(autouse=True)
def reset_service(test_service):
    """Reset service state before each test."""
    test_service.reset()


def create_graph_node(container_name: str, operation_name: str) -> GraphNode:
    """Create a GraphNode for testing."""
    op_sig = OperationSignature(
        operation_name=operation_name,
        input_message_name="Input",
        output_message_name="Output",
    )
    op_list = OperationSignatureList(operation_signature=op_sig)
    node = Node(
        container_name=container_name,
        proto_uri=f"{container_name}.proto",
        image=f"{container_name}:latest",
        node_type="MLModel",
        operation_signature_list=[op_list],
    )
    return GraphNode(node=node, operation=op_list)


def create_single_node_graph() -> ExecutionGraph:
    """Create graph with single node."""
    graph_node = create_graph_node("service-a", "_test_quick")
    return ExecutionGraph(
        all_nodes={graph_node.key: graph_node},
        start_nodes=[graph_node],
    )


def create_chain_graph() -> ExecutionGraph:
    """Create A -> B -> C chain graph."""
    node_a = create_graph_node("service-a", "_test_quick")
    node_b = create_graph_node("service-b", "_test_quick")
    node_c = create_graph_node("service-c", "_test_quick")

    node_a.next_nodes.append(node_b)
    node_b.dependencies.append(node_a)
    node_b.next_nodes.append(node_c)
    node_c.dependencies.append(node_b)

    return ExecutionGraph(
        all_nodes={
            node_a.key: node_a,
            node_b.key: node_b,
            node_c.key: node_c,
        },
        start_nodes=[node_a],
    )


def create_long_running_graph() -> ExecutionGraph:
    """Create graph with long-running task."""
    graph_node = create_graph_node("service-a", "_test_long_running")
    return ExecutionGraph(
        all_nodes={graph_node.key: graph_node},
        start_nodes=[graph_node],
    )


def create_failing_graph() -> ExecutionGraph:
    """Create graph with failing task."""
    graph_node = create_graph_node("service-a", "_test_failing")
    return ExecutionGraph(
        all_nodes={graph_node.key: graph_node},
        start_nodes=[graph_node],
    )


class TestWorkerExecutesSingleTask:
    """Tests for single task execution."""

    def test_worker_executes_single_task(self, engine, worker):
        """Worker executes single task end-to-end."""
        graph = create_single_node_graph()

        engine.initialize_workflow("wf-single", graph)
        engine.start_workflow("wf-single")

        # Process the task
        result = worker.process_task("wf-single", timeout=1)

        assert result is True
        assert engine.is_workflow_complete("wf-single")

        # Check workflow completed successfully
        status = engine.get_workflow_status("wf-single")
        assert status.status.value == "completed"


class TestWorkerExecutesChain:
    """Tests for chain workflow execution."""

    def test_worker_executes_chain(self, engine, worker):
        """Worker executes A -> B -> C chain."""
        graph = create_chain_graph()

        engine.initialize_workflow("wf-chain", graph)
        engine.start_workflow("wf-chain")

        # Run worker until workflow complete
        worker.run("wf-chain", timeout=1)

        assert engine.is_workflow_complete("wf-chain")

        status = engine.get_workflow_status("wf-chain")
        assert status.status.value == "completed"


class TestWorkerHandlesLongRunning:
    """Tests for long-running task handling."""

    def test_worker_handles_long_running(self, engine, worker, test_service):
        """Worker polls long-running task until complete."""
        graph = create_long_running_graph()

        engine.initialize_workflow("wf-long", graph)
        engine.start_workflow("wf-long")

        # Process the task (will poll)
        result = worker.process_task("wf-long", timeout=1)

        assert result is True
        assert engine.is_workflow_complete("wf-long")

        # Verify polling actually happened by checking task progress reached 100
        # The test service increments progress by 50 each get_status call
        # Progress 100 means get_status was called at least twice
        import httpx
        task_id = "svc-" + list(engine._state_store._redis.keys("task:wf-long:*"))[0].decode().split(":")[-1]
        resp = httpx.get(f"{test_service.base_url}/tasks/{task_id}")
        assert resp.status_code == 200
        task_info = resp.json()
        assert task_info["progress"] == 100, "Polling path was not triggered"
        assert task_info["status"] == "complete"


class TestWorkerHandlesFailure:
    """Tests for task failure handling."""

    def test_worker_handles_failure(self, engine, worker):
        """Worker handles failing task."""
        graph = create_failing_graph()

        engine.initialize_workflow("wf-fail", graph)
        engine.start_workflow("wf-fail")

        # Process the task
        result = worker.process_task("wf-fail", timeout=1)

        assert result is True
        assert engine.is_workflow_complete("wf-fail")

        # Check workflow failed
        status = engine.get_workflow_status("wf-fail")
        assert status.status.value == "failed"


class TestWorkerPassesData:
    """Tests for data passing between tasks."""

    def test_worker_passes_output_to_next_task(self, engine, worker):
        """Output from task A becomes input to task B."""
        graph = create_chain_graph()

        engine.initialize_workflow("wf-data", graph)
        engine.start_workflow("wf-data")

        # Process task A
        worker.process_task("wf-data", timeout=1)

        # Claim task B and check it has input from A
        task_b = engine.claim_task("wf-data", timeout=1)
        assert task_b is not None
        assert len(task_b.input_refs) > 0

        # The input should be the output from task A
        input_ref = task_b.input_refs[0]
        assert input_ref.protocol == Protocol.S3
