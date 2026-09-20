"""Unit tests for WorkflowEngine."""

import pytest
import fakeredis

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


@pytest.fixture
def redis_client():
    """Create a fakeredis client."""
    return fakeredis.FakeRedis()


@pytest.fixture
def state_store(redis_client):
    """Create a RedisStateStore instance."""
    return RedisStateStore(redis_client)


@pytest.fixture
def task_queue(redis_client):
    """Create a RedisTaskQueue instance."""
    return RedisTaskQueue(redis_client)


@pytest.fixture
def engine(state_store, task_queue, redis_client):
    """Create a WorkflowEngine instance."""
    return WorkflowEngine(state_store, task_queue, redis_client)


def create_single_node_graph() -> ExecutionGraph:
    """Create a graph with a single node."""
    graph = ExecutionGraph()
    node = create_graph_node("service-a")
    graph.add_node(node)
    graph.start_nodes.append(node)
    return graph


def create_chain_graph() -> ExecutionGraph:
    """Create a linear chain: A -> B -> C."""
    graph = ExecutionGraph()

    node_a = create_graph_node("service-a")
    node_b = create_graph_node("service-b")
    node_c = create_graph_node("service-c")

    node_a.next_nodes.append(node_b)
    node_b.dependencies.append(node_a)

    node_b.next_nodes.append(node_c)
    node_c.dependencies.append(node_b)

    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node(node_c)
    graph.start_nodes.append(node_a)

    return graph


def create_parallel_graph() -> ExecutionGraph:
    """Create parallel branches: A, B (no dependencies)."""
    graph = ExecutionGraph()

    node_a = create_graph_node("service-a")
    node_b = create_graph_node("service-b")

    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.start_nodes.extend([node_a, node_b])

    return graph


def create_fan_out_graph() -> ExecutionGraph:
    """Create fan-out: A -> B, A -> C."""
    graph = ExecutionGraph()

    node_a = create_graph_node("service-a")
    node_b = create_graph_node("service-b")
    node_c = create_graph_node("service-c")

    node_a.next_nodes.extend([node_b, node_c])
    node_b.dependencies.append(node_a)
    node_c.dependencies.append(node_a)

    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node(node_c)
    graph.start_nodes.append(node_a)

    return graph


def create_fan_in_graph() -> ExecutionGraph:
    """Create fan-in: A -> C, B -> C."""
    graph = ExecutionGraph()

    node_a = create_graph_node("service-a")
    node_b = create_graph_node("service-b")
    node_c = create_graph_node("service-c")

    node_a.next_nodes.append(node_c)
    node_b.next_nodes.append(node_c)
    node_c.dependencies.extend([node_a, node_b])

    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node(node_c)
    graph.start_nodes.extend([node_a, node_b])

    return graph


class TestWorkflowEngineInit:
    """Tests for WorkflowEngine initialization."""

    def test_init_with_valid_args(self, state_store, task_queue, redis_client):
        """Engine initializes with valid arguments."""
        engine = WorkflowEngine(state_store, task_queue, redis_client)
        assert engine is not None

    def test_init_without_state_store_raises(self, task_queue, redis_client):
        """Engine raises ValueError without state_store."""
        with pytest.raises(ValueError, match="state_store is required"):
            WorkflowEngine(None, task_queue, redis_client)

    def test_init_without_task_queue_raises(self, state_store, redis_client):
        """Engine raises ValueError without task_queue."""
        with pytest.raises(ValueError, match="task_queue is required"):
            WorkflowEngine(state_store, None, redis_client)

    def test_init_without_redis_client_raises(self, state_store, task_queue):
        """Engine raises ValueError without redis_client."""
        with pytest.raises(ValueError, match="redis_client is required"):
            WorkflowEngine(state_store, task_queue, None)


class TestInitializeWorkflow:
    """Tests for initialize_workflow method."""

    def test_creates_workflow_state(self, engine):
        """Initialize creates workflow in pending state."""
        graph = create_single_node_graph()
        workflow = engine.initialize_workflow("wf-1", graph)

        assert workflow.workflow_id == "wf-1"
        assert workflow.status == WorkflowStatus.PENDING

    def test_creates_tasks_for_all_nodes(self, engine, state_store):
        """Initialize creates tasks for each node."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-1", graph)

        tasks = state_store.get_workflow_tasks("wf-1")
        assert len(tasks) == 3

    def test_tasks_have_correct_node_keys(self, engine, state_store):
        """Tasks have node keys matching graph nodes."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-1", graph)

        tasks = state_store.get_workflow_tasks("wf-1")
        node_keys = {t.node_key for t in tasks}

        assert "service-a:Process" in node_keys
        assert "service-b:Process" in node_keys
        assert "service-c:Process" in node_keys

    def test_empty_workflow_id_raises(self, engine):
        """Initialize raises ValueError for empty workflow_id."""
        graph = create_single_node_graph()
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.initialize_workflow("", graph)

    def test_none_graph_raises(self, engine):
        """Initialize raises ValueError for None graph."""
        with pytest.raises(ValueError, match="graph is required"):
            engine.initialize_workflow("wf-1", None)

    def test_empty_graph_raises(self, engine):
        """Initialize raises ValueError for empty graph."""
        graph = ExecutionGraph()
        with pytest.raises(ValueError, match="graph must have at least one node"):
            engine.initialize_workflow("wf-1", graph)


class TestStartWorkflow:
    """Tests for start_workflow method."""

    def test_sets_workflow_running(self, engine, state_store):
        """Start sets workflow status to running."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        workflow = state_store.get_workflow("wf-1")
        assert workflow.status == WorkflowStatus.RUNNING

    def test_enqueues_tasks_without_dependencies(self, engine, task_queue):
        """Start enqueues tasks with no dependencies."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        assert task_queue.queue_length("wf-1") == 1

    def test_enqueues_all_parallel_tasks(self, engine, task_queue):
        """Start enqueues all tasks in parallel graph."""
        graph = create_parallel_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        assert task_queue.queue_length("wf-1") == 2

    def test_does_not_enqueue_dependent_tasks(self, engine, task_queue):
        """Start does not enqueue tasks with dependencies."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        # Only first node should be enqueued
        assert task_queue.queue_length("wf-1") == 1

    def test_empty_workflow_id_raises(self, engine):
        """Start raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.start_workflow("")

    def test_sets_initial_inputs_on_start_nodes(self, engine):
        """Start sets initial inputs on start nodes."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)

        initial_input = DataReference(
            protocol=Protocol.GRPC,
            uri="upstream-service:50051",
            format="GetConfiguration",
        )
        engine.start_workflow("wf-1", initial_inputs=[initial_input])

        task = engine.claim_task("wf-1", timeout=1)
        assert len(task.input_refs) == 1
        assert task.input_refs[0].uri == "upstream-service:50051"
        assert task.input_refs[0].format == "GetConfiguration"

    def test_sets_initial_inputs_on_all_parallel_start_nodes(self, engine):
        """Start sets initial inputs on all parallel start nodes."""
        graph = create_parallel_graph()
        engine.initialize_workflow("wf-1", graph)

        initial_input = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/input.json",
            format=Format.JSON,
        )
        engine.start_workflow("wf-1", initial_inputs=[initial_input])

        # Both start nodes should receive the initial inputs
        task1 = engine.claim_task("wf-1", timeout=1)
        task2 = engine.claim_task("wf-1", timeout=1)

        assert len(task1.input_refs) == 1
        assert task1.input_refs[0].uri == "s3://bucket/input.json"
        assert len(task2.input_refs) == 1
        assert task2.input_refs[0].uri == "s3://bucket/input.json"

    def test_no_initial_inputs_leaves_start_nodes_empty(self, engine):
        """Start without initial inputs leaves start nodes with empty input_refs."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        assert len(task.input_refs) == 0


class TestClaimTask:
    """Tests for claim_task method."""

    def test_returns_task_state(self, engine):
        """Claim returns task state."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        assert task is not None
        assert task.node_key == "service-a:Process"

    def test_marks_task_running(self, engine, state_store):
        """Claim sets task status to running."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        retrieved = state_store.get_task("wf-1", task.task_id)
        assert retrieved.status == TaskStatus.RUNNING

    def test_returns_none_when_empty(self, engine):
        """Claim returns None when no tasks available."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        # Note: not starting workflow, so no tasks enqueued

        task = engine.claim_task("wf-1", timeout=1)
        assert task is None

    def test_empty_workflow_id_raises(self, engine):
        """Claim raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.claim_task("")


class TestCompleteTask:
    """Tests for complete_task method."""

    def test_marks_task_completed(self, engine, state_store):
        """Complete sets task status to completed."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        completed = engine.complete_task("wf-1", task.task_id)

        assert completed.status == TaskStatus.COMPLETED

    def test_stores_output_refs(self, engine, state_store):
        """Complete stores output references."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        output_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/output.json",
            format=Format.JSON,
        )
        completed = engine.complete_task("wf-1", task.task_id, output_refs=[output_ref])

        assert len(completed.output_refs) == 1
        assert completed.output_refs[0].uri == "s3://bucket/output.json"

    def test_passes_output_to_dependent_input(self, engine):
        """Complete passes output refs to dependent task as input refs."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        # Complete task A with output
        task_a = engine.claim_task("wf-1", timeout=1)
        output_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/a-output.json",
            format=Format.JSON,
        )
        engine.complete_task("wf-1", task_a.task_id, output_refs=[output_ref])

        # Task B should receive A's output as input
        task_b = engine.claim_task("wf-1", timeout=1)
        assert len(task_b.input_refs) == 1
        assert task_b.input_refs[0].uri == "s3://bucket/a-output.json"

    def test_fan_in_collects_all_inputs(self, engine):
        """Fan-in task receives outputs from all dependencies."""
        graph = create_fan_in_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        # Complete A and B with different outputs
        task_a = engine.claim_task("wf-1", timeout=1)
        task_b = engine.claim_task("wf-1", timeout=1)

        output_a = DataReference(
            protocol=Protocol.S3, uri="s3://bucket/a.json", format=Format.JSON
        )
        output_b = DataReference(
            protocol=Protocol.S3, uri="s3://bucket/b.json", format=Format.JSON
        )

        engine.complete_task("wf-1", task_a.task_id, output_refs=[output_a])
        engine.complete_task("wf-1", task_b.task_id, output_refs=[output_b])

        # Task C should have both A and B outputs as inputs
        task_c = engine.claim_task("wf-1", timeout=1)
        assert len(task_c.input_refs) == 2
        uris = {ref.uri for ref in task_c.input_refs}
        assert "s3://bucket/a.json" in uris
        assert "s3://bucket/b.json" in uris

    def test_enqueues_ready_dependent(self, engine, task_queue):
        """Complete enqueues dependent task when ready."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task_a = engine.claim_task("wf-1", timeout=1)
        engine.complete_task("wf-1", task_a.task_id)

        # Task B should now be enqueued
        assert task_queue.queue_length("wf-1") == 1
        task_b = engine.claim_task("wf-1", timeout=1)
        assert task_b.node_key == "service-b:Process"

    def test_fan_in_waits_for_all_dependencies(self, engine, task_queue):
        """Fan-in task waits for all dependencies."""
        graph = create_fan_in_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        # Claim and complete task A
        task_a = engine.claim_task("wf-1", timeout=1)
        engine.complete_task("wf-1", task_a.task_id)

        # Task C should not be enqueued yet (waiting for B)
        # Only task B should be in queue
        task_b = engine.claim_task("wf-1", timeout=1)
        assert task_b is not None

        # After completing B, C should be enqueued
        engine.complete_task("wf-1", task_b.task_id)
        task_c = engine.claim_task("wf-1", timeout=1)
        assert task_c.node_key == "service-c:Process"

    def test_completes_workflow_when_all_done(self, engine, state_store):
        """Complete marks workflow as completed when all tasks done."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        engine.complete_task("wf-1", task.task_id)

        workflow = state_store.get_workflow("wf-1")
        assert workflow.status == WorkflowStatus.COMPLETED

    def test_empty_workflow_id_raises(self, engine):
        """Complete raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.complete_task("", "task-1")

    def test_empty_task_id_raises(self, engine):
        """Complete raises ValueError for empty task_id."""
        with pytest.raises(ValueError, match="task_id is required"):
            engine.complete_task("wf-1", "")


class TestFailTask:
    """Tests for fail_task method."""

    def test_marks_task_failed(self, engine, state_store):
        """Fail sets task status to failed."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        failed = engine.fail_task("wf-1", task.task_id, "Test error")

        assert failed.status == TaskStatus.FAILED
        assert failed.error == "Test error"

    def test_fails_workflow(self, engine, state_store):
        """Fail sets workflow status to failed."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        engine.fail_task("wf-1", task.task_id, "Test error")

        workflow = state_store.get_workflow("wf-1")
        assert workflow.status == WorkflowStatus.FAILED
        assert "Test error" in workflow.error

    def test_empty_workflow_id_raises(self, engine):
        """Fail raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.fail_task("", "task-1", "error")

    def test_empty_task_id_raises(self, engine):
        """Fail raises ValueError for empty task_id."""
        with pytest.raises(ValueError, match="task_id is required"):
            engine.fail_task("wf-1", "", "error")

    def test_empty_error_raises(self, engine):
        """Fail raises ValueError for empty error."""
        with pytest.raises(ValueError, match="error is required"):
            engine.fail_task("wf-1", "task-1", "")


class TestGetWorkflowStatus:
    """Tests for get_workflow_status method."""

    def test_returns_workflow_state(self, engine):
        """Get status returns workflow state."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)

        status = engine.get_workflow_status("wf-1")
        assert status.workflow_id == "wf-1"
        assert status.status == WorkflowStatus.PENDING

    def test_empty_workflow_id_raises(self, engine):
        """Get status raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.get_workflow_status("")


class TestIsWorkflowComplete:
    """Tests for is_workflow_complete method."""

    def test_returns_false_for_pending(self, engine):
        """Is complete returns False for pending workflow."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)

        assert engine.is_workflow_complete("wf-1") is False

    def test_returns_false_for_running(self, engine):
        """Is complete returns False for running workflow."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        assert engine.is_workflow_complete("wf-1") is False

    def test_returns_true_for_completed(self, engine):
        """Is complete returns True for completed workflow."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        engine.complete_task("wf-1", task.task_id)

        assert engine.is_workflow_complete("wf-1") is True

    def test_returns_true_for_failed(self, engine):
        """Is complete returns True for failed workflow."""
        graph = create_single_node_graph()
        engine.initialize_workflow("wf-1", graph)
        engine.start_workflow("wf-1")

        task = engine.claim_task("wf-1", timeout=1)
        engine.fail_task("wf-1", task.task_id, "error")

        assert engine.is_workflow_complete("wf-1") is True

    def test_empty_workflow_id_raises(self, engine):
        """Is complete raises ValueError for empty workflow_id."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            engine.is_workflow_complete("")


class TestChainWorkflow:
    """Tests for linear chain workflow execution."""

    def test_full_chain_execution(self, engine, state_store):
        """Execute complete chain workflow."""
        graph = create_chain_graph()
        engine.initialize_workflow("wf-chain", graph)
        engine.start_workflow("wf-chain")

        # Execute all three tasks in sequence
        for expected_node in ["service-a", "service-b", "service-c"]:
            task = engine.claim_task("wf-chain", timeout=1)
            assert task.node_key.startswith(expected_node)
            engine.complete_task("wf-chain", task.task_id)

        workflow = state_store.get_workflow("wf-chain")
        assert workflow.status == WorkflowStatus.COMPLETED


class TestFanOutWorkflow:
    """Tests for fan-out workflow execution."""

    def test_fan_out_execution(self, engine, task_queue):
        """Execute fan-out workflow."""
        graph = create_fan_out_graph()
        engine.initialize_workflow("wf-fanout", graph)
        engine.start_workflow("wf-fanout")

        # Execute A
        task_a = engine.claim_task("wf-fanout", timeout=1)
        assert task_a.node_key == "service-a:Process"
        engine.complete_task("wf-fanout", task_a.task_id)

        # Both B and C should be enqueued
        assert task_queue.queue_length("wf-fanout") == 2


class TestFanInWorkflow:
    """Tests for fan-in workflow execution."""

    def test_fan_in_execution(self, engine, state_store):
        """Execute fan-in workflow."""
        graph = create_fan_in_graph()
        engine.initialize_workflow("wf-fanin", graph)
        engine.start_workflow("wf-fanin")

        # Execute A and B (parallel start nodes)
        task1 = engine.claim_task("wf-fanin", timeout=1)
        task2 = engine.claim_task("wf-fanin", timeout=1)

        engine.complete_task("wf-fanin", task1.task_id)
        engine.complete_task("wf-fanin", task2.task_id)

        # C should now be ready
        task_c = engine.claim_task("wf-fanin", timeout=1)
        assert task_c.node_key == "service-c:Process"
        engine.complete_task("wf-fanin", task_c.task_id)

        workflow = state_store.get_workflow("wf-fanin")
        assert workflow.status == WorkflowStatus.COMPLETED


class TestBlockedTasks:
    """Tests for deferring tasks on unhealthy services and resuming them."""

    def start(self, engine, graph, workflow_id="wf-blk"):
        engine.initialize_workflow(workflow_id, graph)
        engine.start_workflow(workflow_id)
        return workflow_id

    def test_defer_blocks_task_without_failing_workflow(self, engine, state_store, task_queue):
        wf = self.start(engine, create_single_node_graph())
        task = engine.claim_task(wf, timeout=1)

        engine.defer_task(wf, task.task_id, "circuit open", delay=3600)

        assert state_store.get_task(wf, task.task_id).status == TaskStatus.BLOCKED
        assert state_store.get_task(wf, task.task_id).error == "circuit open"
        assert state_store.get_workflow(wf).status == WorkflowStatus.RUNNING
        assert engine.is_workflow_complete(wf) is False
        assert task_queue.delayed_tasks(wf) == [task.task_id]
        assert engine.claim_task(wf, timeout=0) is None

    def test_release_due_tasks_requeues_as_pending(self, engine, state_store):
        wf = self.start(engine, create_single_node_graph())
        task = engine.claim_task(wf, timeout=1)
        engine.defer_task(wf, task.task_id, "circuit open", delay=0)

        assert engine.release_due_tasks(wf) == 1

        state = state_store.get_task(wf, task.task_id)
        assert state.status == TaskStatus.PENDING
        assert state.error is None
        assert engine.claim_task(wf, timeout=1).task_id == task.task_id

    def test_release_due_tasks_skips_future(self, engine):
        wf = self.start(engine, create_single_node_graph())
        task = engine.claim_task(wf, timeout=1)
        engine.defer_task(wf, task.task_id, "circuit open", delay=3600)

        assert engine.release_due_tasks(wf) == 0

    def test_resume_only_tasks_of_named_service(self, engine, state_store, task_queue):
        wf = self.start(engine, create_parallel_graph())
        first = engine.claim_task(wf, timeout=1)
        second = engine.claim_task(wf, timeout=1)
        for task in (first, second):
            engine.defer_task(wf, task.task_id, "down", delay=3600)

        resumed = engine.resume_blocked_tasks(wf, "service-a")

        assert resumed == 1
        claimed = engine.claim_task(wf, timeout=1)
        assert claimed.node_key.startswith("service-a:")
        remaining = task_queue.delayed_tasks(wf)
        assert len(remaining) == 1
        assert state_store.get_task(wf, remaining[0]).node_key.startswith("service-b:")

    def test_resume_does_not_match_name_prefix(self, engine):
        # "service-a" must not resume tasks of a service called "service-ab"
        graph = ExecutionGraph()
        node = create_graph_node("service-ab")
        graph.add_node(node)
        graph.start_nodes.append(node)
        wf = self.start(engine, graph)
        task = engine.claim_task(wf, timeout=1)
        engine.defer_task(wf, task.task_id, "down", delay=3600)

        assert engine.resume_blocked_tasks(wf, "service-a") == 0

    def test_resume_ignores_non_running_workflow(self, engine, state_store):
        wf = self.start(engine, create_single_node_graph())
        task = engine.claim_task(wf, timeout=1)
        engine.defer_task(wf, task.task_id, "down", delay=3600)
        state_store.update_workflow_status(wf, WorkflowStatus.FAILED)

        assert engine.resume_blocked_tasks(wf, "service-a") == 0

    def test_blocked_chain_completes_after_resume(self, engine, state_store):
        wf = self.start(engine, create_chain_graph())
        a = engine.claim_task(wf, timeout=1)
        engine.complete_task(wf, a.task_id)
        b = engine.claim_task(wf, timeout=1)
        engine.defer_task(wf, b.task_id, "down", delay=3600)

        engine.resume_blocked_tasks(wf, "service-b")
        for _ in range(2):
            task = engine.claim_task(wf, timeout=1)
            engine.complete_task(wf, task.task_id)

        assert state_store.get_workflow(wf).status == WorkflowStatus.COMPLETED

    def test_defer_validation(self, engine):
        with pytest.raises(ValueError, match="workflow_id"):
            engine.defer_task("", "t", "r", delay=0)
        with pytest.raises(ValueError, match="task_id"):
            engine.defer_task("wf", "", "r", delay=0)
        with pytest.raises(ValueError, match="reason"):
            engine.defer_task("wf", "t", "", delay=0)
        with pytest.raises(ValueError, match="service_name"):
            engine.resume_blocked_tasks("wf", "")


class TestErrorSanitising:
    """Errors stored on tasks and workflows are redacted and capped."""

    def failing_workflow(self, engine, error, workflow_id="wf-san"):
        engine.initialize_workflow(workflow_id, create_single_node_graph())
        engine.start_workflow(workflow_id)
        task = engine.claim_task(workflow_id, timeout=1)
        engine.fail_task(workflow_id, task.task_id, error)
        return workflow_id, task.task_id

    def test_email_removed_from_task_and_workflow(self, engine, state_store):
        wf, task_id = self.failing_workflow(engine, "bad row for jane.doe@example.eu")

        assert "jane.doe@example.eu" not in state_store.get_task(wf, task_id).error
        assert "jane.doe@example.eu" not in state_store.get_workflow(wf).error

    def test_long_error_truncated(self, engine, state_store):
        wf, task_id = self.failing_workflow(engine, "x" * 2000)

        assert len(state_store.get_task(wf, task_id).error) < 600

    def test_ordinary_error_unchanged(self, engine, state_store):
        message = "Connection failed: [Errno 111] Connection refused"
        wf, task_id = self.failing_workflow(engine, message)

        assert state_store.get_task(wf, task_id).error == message

    def test_blocked_reason_sanitised(self, engine, state_store):
        engine.initialize_workflow("wf-san2", create_single_node_graph())
        engine.start_workflow("wf-san2")
        task = engine.claim_task("wf-san2", timeout=1)

        engine.defer_task("wf-san2", task.task_id, "down for user@example.eu", delay=60)

        assert "user@example.eu" not in state_store.get_task("wf-san2", task.task_id).error


class TestBlockedAttempts:
    """The engine counts deferrals per task and clears them when it ends."""

    def start(self, engine, workflow_id="wf-att"):
        engine.initialize_workflow(workflow_id, create_single_node_graph())
        engine.start_workflow(workflow_id)
        return workflow_id, engine.claim_task(workflow_id, timeout=1)

    def test_counts_deferrals(self, engine):
        wf, task = self.start(engine)

        assert engine.record_blocked_attempt(wf, task.task_id) == 1
        assert engine.record_blocked_attempt(wf, task.task_id) == 2

    def test_completion_clears_count(self, engine, task_queue):
        wf, task = self.start(engine)
        engine.record_blocked_attempt(wf, task.task_id)
        engine.record_blocked_attempt(wf, task.task_id)

        engine.complete_task(wf, task.task_id)

        assert task_queue.increment_attempts(wf, task.task_id) == 1

    def test_failure_clears_count(self, engine, task_queue):
        wf, task = self.start(engine, "wf-att2")
        engine.record_blocked_attempt(wf, task.task_id)

        engine.fail_task(wf, task.task_id, "boom")

        assert task_queue.increment_attempts(wf, task.task_id) == 1


class TestBlockedAge:
    """How long a task has been blocked, for the maximum-blocked-time limit."""

    def start(self, engine, workflow_id="wf-age"):
        engine.initialize_workflow(workflow_id, create_single_node_graph())
        engine.start_workflow(workflow_id)
        return workflow_id, engine.claim_task(workflow_id, timeout=1)

    def test_zero_on_first_block(self, engine):
        wf, task = self.start(engine)

        assert engine.blocked_age(wf, task.task_id) < 1

    def test_grows_from_the_first_block(self, engine, task_queue):
        wf, task = self.start(engine)
        import time

        task_queue.mark_blocked(wf, task.task_id, now=time.time() - 120)

        assert 119 < engine.blocked_age(wf, task.task_id) < 130

    def test_completion_resets_it(self, engine, task_queue):
        import time

        wf, task = self.start(engine, "wf-age2")
        task_queue.mark_blocked(wf, task.task_id, now=time.time() - 120)
        engine.complete_task(wf, task.task_id)

        assert engine.blocked_age(wf, task.task_id) < 1

    def test_validation(self, engine):
        with pytest.raises(ValueError, match="workflow_id"):
            engine.blocked_age("", "t")
        with pytest.raises(ValueError, match="task_id"):
            engine.blocked_age("wf", "")
