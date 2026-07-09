"""Integration tests for WorkflowEngine with real Redis."""

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from models.data_reference import DataReference, Format, Protocol
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import TaskStatus, WorkflowStatus
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine


def create_node(name: str, operation: str = "Process") -> Node:
    """Helper to create a test node."""
    return Node(
        container_name=name,
        proto_uri=f"{name}.proto",
        image=f"{name}:latest",
        node_type="microservice",
        operation_signature_list=[
            OperationSignatureList(
                operation_signature=OperationSignature(
                    operation_name=operation,
                    input_message_name="Input",
                    output_message_name="Output",
                )
            )
        ],
    )


def create_graph_node(name: str, operation: str = "Process") -> GraphNode:
    """Helper to create a test graph node."""
    node = create_node(name, operation)
    return GraphNode(
        node=node,
        operation=node.operation_signature_list[0],
    )


def create_diamond_graph() -> ExecutionGraph:
    """Create diamond: A -> B, A -> C, B -> D, C -> D."""
    graph = ExecutionGraph()

    node_a = create_graph_node("service-a")
    node_b = create_graph_node("service-b")
    node_c = create_graph_node("service-c")
    node_d = create_graph_node("service-d")

    node_a.next_nodes.extend([node_b, node_c])
    node_b.dependencies.append(node_a)
    node_c.dependencies.append(node_a)

    node_b.next_nodes.append(node_d)
    node_c.next_nodes.append(node_d)
    node_d.dependencies.extend([node_b, node_c])

    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node(node_c)
    graph.add_node(node_d)
    graph.start_nodes.append(node_a)

    return graph


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


class TestFullWorkflowExecution:
    """Integration tests for complete workflow execution."""

    def test_diamond_workflow_execution(self, engine, redis_client):
        """Execute diamond-shaped workflow with real Redis."""
        graph = create_diamond_graph()
        engine.initialize_workflow("wf-diamond", graph)
        engine.start_workflow("wf-diamond")

        # Step 1: Execute A
        task_a = engine.claim_task("wf-diamond", timeout=1)
        assert task_a.node_key == "service-a:Process"
        output_a = DataReference(
            protocol=Protocol.S3, uri="s3://bucket/a.json", format=Format.JSON
        )
        engine.complete_task("wf-diamond", task_a.task_id, output_refs=[output_a])

        # Step 2: Execute B and C (both ready after A completes)
        task_b = engine.claim_task("wf-diamond", timeout=1)
        task_c = engine.claim_task("wf-diamond", timeout=1)

        # Complete both B and C
        engine.complete_task("wf-diamond", task_b.task_id)
        engine.complete_task("wf-diamond", task_c.task_id)

        # Step 3: Execute D (ready after both B and C complete)
        task_d = engine.claim_task("wf-diamond", timeout=1)
        assert task_d.node_key == "service-d:Process"
        engine.complete_task("wf-diamond", task_d.task_id)

        # Verify workflow completed
        assert engine.is_workflow_complete("wf-diamond")
        status = engine.get_workflow_status("wf-diamond")
        assert status.status == WorkflowStatus.COMPLETED

    def test_workflow_with_data_passing(self, engine, redis_client):
        """Execute workflow with data references passed between tasks."""
        graph = ExecutionGraph()
        node_a = create_graph_node("producer")
        node_b = create_graph_node("consumer")

        node_a.next_nodes.append(node_b)
        node_b.dependencies.append(node_a)

        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.start_nodes.append(node_a)

        engine.initialize_workflow("wf-data", graph)
        engine.start_workflow("wf-data")

        # Producer creates output
        task_a = engine.claim_task("wf-data", timeout=1)
        output = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/data.parquet",
            format=Format.PARQUET,
            size_bytes=1024,
            checksum="sha256:abc123",
        )
        engine.complete_task("wf-data", task_a.task_id, output_refs=[output])

        # Consumer task receives producer's output as input
        task_b = engine.claim_task("wf-data", timeout=1)
        assert task_b is not None
        assert len(task_b.input_refs) == 1
        assert task_b.input_refs[0].uri == "s3://bucket/data.parquet"
        assert task_b.input_refs[0].format == Format.PARQUET

        engine.complete_task("wf-data", task_b.task_id)
        assert engine.is_workflow_complete("wf-data")


class TestWorkflowFailureHandling:
    """Integration tests for workflow failure scenarios."""

    def test_task_failure_fails_workflow(self, engine, redis_client):
        """Task failure propagates to workflow failure."""
        graph = ExecutionGraph()
        node = create_graph_node("failing-service")
        graph.add_node(node)
        graph.start_nodes.append(node)

        engine.initialize_workflow("wf-fail", graph)
        engine.start_workflow("wf-fail")

        task = engine.claim_task("wf-fail", timeout=1)
        engine.fail_task("wf-fail", task.task_id, "Service unavailable")

        assert engine.is_workflow_complete("wf-fail")
        status = engine.get_workflow_status("wf-fail")
        assert status.status == WorkflowStatus.FAILED
        assert "Service unavailable" in status.error


class TestDataPersistence:
    """Integration tests for data persistence."""

    def test_workflow_state_survives_reconnect(self, redis_container):
        """Workflow state persists across connections."""
        # First connection: initialize and start workflow
        client1 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        engine1 = WorkflowEngine(
            RedisStateStore(client1), RedisTaskQueue(client1), client1
        )

        graph = ExecutionGraph()
        node = create_graph_node("persistent-service")
        graph.add_node(node)
        graph.start_nodes.append(node)

        engine1.initialize_workflow("wf-persist", graph)
        engine1.start_workflow("wf-persist")
        client1.close()

        # Second connection: verify and continue
        client2 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        engine2 = WorkflowEngine(
            RedisStateStore(client2), RedisTaskQueue(client2), client2
        )

        status = engine2.get_workflow_status("wf-persist")
        assert status.status == WorkflowStatus.RUNNING

        task = engine2.claim_task("wf-persist", timeout=1)
        assert task is not None
        engine2.complete_task("wf-persist", task.task_id)

        assert engine2.is_workflow_complete("wf-persist")
        client2.close()
