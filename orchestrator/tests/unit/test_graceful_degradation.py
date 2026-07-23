"""TDD specification for graceful degradation of workflows.

These tests are written **before** the implementation exists, following a
test-driven approach. They describe the target behavior for four capabilities
the orchestrator does not have yet:

1. **Optional nodes**: a node marked optional whose failure must not fail the
   whole workflow.
2. **Skip conditions**: a node that is skipped (not executed) when a declared
   condition is not met, without being treated as a failure.
3. **Fallback nodes**: a node that, on failure, hands off to a designated
   fallback so the workflow can continue.
4. **Partial workflow completion**: a workflow that finishes in a
   ``completed_with_errors`` / partial state when some optional work was
   skipped or failed but all *required* work succeeded.

Because the feature is unimplemented, every test here is marked
``xfail(strict=True)``: it is expected to fail today, and pytest will flag it
(as an ``XPASS`` failure) the moment the behavior starts working, prompting
whoever implements the feature to remove the marker and promote the test to a
regular passing test.

The current engine (see ``services.workflow_engine.WorkflowEngine``) fails the
entire workflow on any task failure and has no notion of optional/skip/fallback
nodes or a ``SKIPPED`` task status; the assertions below encode where that is
expected to change.
"""

import fakeredis
import pytest

from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import TaskStatus, WorkflowStatus
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine

pytestmark = pytest.mark.graceful_degradation


# ---------------------------------------------------------------------------
# Test helpers: build small in-memory workflows without Redis or services.
# ---------------------------------------------------------------------------
def make_engine() -> WorkflowEngine:
    """Create a WorkflowEngine backed by an in-memory fake Redis."""
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    return WorkflowEngine(state_store, task_queue, redis_client)


def make_graph_node(container_name: str, operation_name: str = "run") -> GraphNode:
    """Create a single GraphNode for a container/operation."""
    op = OperationSignatureList(
        operation_signature=OperationSignature(operation_name=operation_name)
    )
    node = Node(
        container_name=container_name,
        proto_uri="proto://x",
        image="image:latest",
        node_type="python",
        operation_signature_list=[op],
    )
    return GraphNode(node=node, operation=op)


def link(upstream: GraphNode, downstream: GraphNode) -> None:
    """Wire ``downstream`` to depend on ``upstream``."""
    upstream.next_nodes.append(downstream)
    downstream.dependencies.append(upstream)


def task_id_for(engine: WorkflowEngine, node: GraphNode) -> str:
    """Compute the engine's task id for a graph node."""
    return engine._task_id_from_node_key(node.key)


# ---------------------------------------------------------------------------
# 1. OPTIONAL NODES
# ---------------------------------------------------------------------------
class TestOptionalNodes:
    """An optional node's failure should not fail the workflow."""

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: optional-node failure "
        "still fails the whole workflow",
    )
    def test_optional_node_failure_does_not_fail_workflow(self):
        """A -> B(optional). B fails; workflow should NOT be FAILED."""
        engine = make_engine()
        a = make_graph_node("required-A")
        b = make_graph_node("optional-B")
        link(a, b)

        graph = ExecutionGraph()
        graph.add_node(a)
        graph.add_node(b)
        graph.start_nodes = [a]

        engine.initialize_workflow("wf-opt", graph)
        # TARGET API: mark node B optional at (or after) initialization.
        engine.mark_optional("wf-opt", task_id_for(engine, b))  # noqa: F821

        engine.start_workflow("wf-opt")
        engine.complete_task("wf-opt", task_id_for(engine, a))

        # B fails, but because it is optional the workflow must not fail.
        engine.fail_task("wf-opt", task_id_for(engine, b), "optional service down")

        status = engine.get_workflow_status("wf-opt").status
        assert status != WorkflowStatus.FAILED

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: no distinct handling for "
        "required-node failure vs optional",
    )
    def test_required_node_failure_still_fails_workflow(self):
        """A required node failing must still fail the workflow (regression guard)."""
        engine = make_engine()
        a = make_graph_node("required-A")
        b = make_graph_node("required-B")
        link(a, b)

        graph = ExecutionGraph()
        graph.add_node(a)
        graph.add_node(b)
        graph.start_nodes = [a]

        engine.initialize_workflow("wf-req", graph)
        engine.start_workflow("wf-req")
        engine.complete_task("wf-req", task_id_for(engine, a))

        # TARGET API: is_required reflects that B was never marked optional.
        assert engine.is_required("wf-req", task_id_for(engine, b)) is True  # noqa: F821

        engine.fail_task("wf-req", task_id_for(engine, b), "hard failure")
        assert engine.get_workflow_status("wf-req").status == WorkflowStatus.FAILED


# ---------------------------------------------------------------------------
# 2. SKIP CONDITIONS
# ---------------------------------------------------------------------------
class TestSkipConditions:
    """A node whose condition is not met is skipped, not failed."""

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: no SKIPPED status or "
        "skip_task on the engine",
    )
    def test_skipped_status_exists(self):
        """There should be a dedicated SKIPPED task status."""
        assert hasattr(TaskStatus, "SKIPPED")  # noqa: F821

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: skip_task does not exist",
    )
    def test_skip_condition_marks_task_skipped(self):
        """A -> B. B is skipped by condition; B ends SKIPPED, not FAILED."""
        engine = make_engine()
        a = make_graph_node("A")
        b = make_graph_node("conditional-B")
        link(a, b)

        graph = ExecutionGraph()
        graph.add_node(a)
        graph.add_node(b)
        graph.start_nodes = [a]

        engine.initialize_workflow("wf-skip", graph)
        engine.start_workflow("wf-skip")
        engine.complete_task("wf-skip", task_id_for(engine, a))

        # TARGET API: skip a task whose declared condition evaluated false.
        engine.skip_task(  # noqa: F821
            "wf-skip",
            task_id_for(engine, b),
            reason="condition not met",
        )

        b_task = engine._state_store.get_task("wf-skip", task_id_for(engine, b))
        assert b_task.status == TaskStatus.SKIPPED  # noqa: F821

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: skipping should still "
        "enqueue downstream dependents",
    )
    def test_skipped_node_still_releases_dependents(self):
        """A -> B(skipped) -> C. Skipping B should let C become ready."""
        engine = make_engine()
        a = make_graph_node("A")
        b = make_graph_node("skippable-B")
        c = make_graph_node("C")
        link(a, b)
        link(b, c)

        graph = ExecutionGraph()
        for n in (a, b, c):
            graph.add_node(n)
        graph.start_nodes = [a]

        engine.initialize_workflow("wf-skip2", graph)
        engine.start_workflow("wf-skip2")
        engine.complete_task("wf-skip2", task_id_for(engine, a))

        engine.skip_task(  # noqa: F821
            "wf-skip2", task_id_for(engine, b), reason="condition not met"
        )

        # C's dependency on B should be satisfied by the skip, so C is queued.
        c_task = engine.claim_task("wf-skip2")
        assert c_task is not None
        assert c_task.task_id == task_id_for(engine, c)


# ---------------------------------------------------------------------------
# 3. FALLBACK NODES
# ---------------------------------------------------------------------------
class TestFallbackNodes:
    """On failure, a node hands off to a designated fallback node."""

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: no fallback registration "
        "or activation on the engine",
    )
    def test_fallback_activated_on_primary_failure(self):
        """primary fails -> its registered fallback is enqueued."""
        engine = make_engine()
        primary = make_graph_node("primary")
        fallback = make_graph_node("fallback")

        graph = ExecutionGraph()
        graph.add_node(primary)
        graph.add_node(fallback)
        graph.start_nodes = [primary, fallback]

        engine.initialize_workflow("wf-fb", graph)

        # TARGET API: declare fallback as the fallback for primary.
        engine.register_fallback(  # noqa: F821
            "wf-fb",
            task_id_for(engine, primary),
            task_id_for(engine, fallback),
        )

        engine.start_workflow("wf-fb")
        # Drain the start node(s) we don't care about, then fail primary.
        engine.fail_task("wf-fb", task_id_for(engine, primary), "primary crashed")

        # The fallback should now be queued to run in primary's place.
        queued = engine.claim_task("wf-fb")
        assert queued is not None
        assert queued.task_id == task_id_for(engine, fallback)
        # Primary failing into a fallback must not fail the workflow outright.
        assert engine.get_workflow_status("wf-fb").status != WorkflowStatus.FAILED

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: fallback success should "
        "let the workflow complete",
    )
    def test_successful_fallback_allows_completion(self):
        """If the fallback succeeds, the workflow completes (with errors noted)."""
        engine = make_engine()
        primary = make_graph_node("primary")
        fallback = make_graph_node("fallback")

        graph = ExecutionGraph()
        graph.add_node(primary)
        graph.add_node(fallback)
        graph.start_nodes = [primary, fallback]

        engine.initialize_workflow("wf-fb2", graph)
        engine.register_fallback(  # noqa: F821
            "wf-fb2",
            task_id_for(engine, primary),
            task_id_for(engine, fallback),
        )
        engine.start_workflow("wf-fb2")
        engine.fail_task("wf-fb2", task_id_for(engine, primary), "primary crashed")

        # Run the fallback to success.
        fb_task = engine.claim_task("wf-fb2")
        engine.complete_task("wf-fb2", fb_task.task_id)

        status = engine.get_workflow_status("wf-fb2").status
        assert status in (
            WorkflowStatus.COMPLETED,
            getattr(WorkflowStatus, "COMPLETED_WITH_ERRORS", WorkflowStatus.COMPLETED),
        )


# ---------------------------------------------------------------------------
# 4. PARTIAL WORKFLOW COMPLETION
# ---------------------------------------------------------------------------
class TestPartialCompletion:
    """A workflow can finish in a partial/degraded terminal state."""

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: no COMPLETED_WITH_ERRORS "
        "workflow status",
    )
    def test_completed_with_errors_status_exists(self):
        """There should be a terminal status for partial success."""
        assert hasattr(WorkflowStatus, "COMPLETED_WITH_ERRORS")  # noqa: F821

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: optional failure should "
        "yield partial completion, not FAILED",
    )
    def test_optional_failure_yields_partial_completion(self):
        """Required A completes, optional B fails -> workflow COMPLETED_WITH_ERRORS."""
        engine = make_engine()
        a = make_graph_node("required-A")
        b = make_graph_node("optional-B")
        # A and B are independent leaves so A can complete on its own.
        graph = ExecutionGraph()
        graph.add_node(a)
        graph.add_node(b)
        graph.start_nodes = [a, b]

        engine.initialize_workflow("wf-part", graph)
        engine.mark_optional("wf-part", task_id_for(engine, b))  # noqa: F821
        engine.start_workflow("wf-part")

        engine.complete_task("wf-part", task_id_for(engine, a))
        engine.fail_task("wf-part", task_id_for(engine, b), "optional down")

        status = engine.get_workflow_status("wf-part").status
        assert status == WorkflowStatus.COMPLETED_WITH_ERRORS  # noqa: F821

    @pytest.mark.xfail(
        strict=True,
        reason="graceful degradation not implemented: is_workflow_complete does "
        "not recognize partial terminal states",
    )
    def test_partial_completion_counts_as_complete(self):
        """A workflow in a partial terminal state is 'complete' for polling."""
        engine = make_engine()
        a = make_graph_node("required-A")
        b = make_graph_node("optional-B")
        graph = ExecutionGraph()
        graph.add_node(a)
        graph.add_node(b)
        graph.start_nodes = [a, b]

        engine.initialize_workflow("wf-part2", graph)
        engine.mark_optional("wf-part2", task_id_for(engine, b))  # noqa: F821
        engine.start_workflow("wf-part2")
        engine.complete_task("wf-part2", task_id_for(engine, a))
        engine.fail_task("wf-part2", task_id_for(engine, b), "optional down")

        # Workers poll is_workflow_complete(); a degraded finish must count.
        assert engine.is_workflow_complete("wf-part2") is True