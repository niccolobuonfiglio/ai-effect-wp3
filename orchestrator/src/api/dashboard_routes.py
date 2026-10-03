"""Read-only routes that feed the dashboard page (``/ui``).

Endpoints:
    GET /workflows                      list of workflows, newest first
    GET /workflows/{workflow_id}/graph  nodes and edges with live status
    GET /workflows/{workflow_id}/logs   log lines mentioning the workflow
    GET /orchestrator/alerts            recent alerts from all processes

"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from api.models import (
    AlertListResponse,
    ErrorResponse,
    GraphEdgeResponse,
    GraphNodeResponse,
    WorkflowGraphResponse,
    WorkflowListResponse,
    WorkflowSummaryResponse,
)
from services.log_reader import MAX_LINES, LogReader
from services.service_registry import ServiceRegistry
from services.state_store import WorkflowNotFoundError
from services.workflow_catalog import WorkflowCatalog
from services.workflow_engine import WorkflowEngine


def _graph_nodes(blueprint: dict) -> tuple[list[dict], list[tuple[str, str]]]:
    """Nodes and edges of a blueprint, keyed like the engine's node_keys."""
    nodes: list[dict] = []
    edges: list[tuple[str, str]] = []
    for node in blueprint.get("nodes", []) or []:
        container = node.get("container_name") or "?"
        node_type = node.get("node_type")
        operations = node.get("operation_signature_list") or []
        if not operations:
            nodes.append(
                {"key": container, "container_name": container, "operation": None, "node_type": node_type}
            )
            continue
        for op in operations:
            name = (op.get("operation_signature") or {}).get("operation_name")
            key = f"{container}:{name}"
            nodes.append(
                {"key": key, "container_name": container, "operation": name, "node_type": node_type}
            )
            for conn in op.get("connected_to") or []:
                target_op = (conn.get("operation_signature") or {}).get("operation_name")
                edges.append((key, f"{conn.get('container_name')}:{target_op}"))
    return nodes, edges


def create_dashboard_router(
    engine: WorkflowEngine,
    catalog: WorkflowCatalog,
    registry: ServiceRegistry,
    log_reader: LogReader | None = None,
    dependencies: list | None = None,
) -> APIRouter:
    """Build the dashboard router.
    """
    if engine is None:
        raise ValueError("engine is required")
    if catalog is None:
        raise ValueError("catalog is required")
    if registry is None:
        raise ValueError("registry is required")
    reader = log_reader or LogReader()

    router = APIRouter(tags=["dashboard"], dependencies=list(dependencies or []))

    @router.get("/workflows", response_model=WorkflowListResponse, summary="List workflows")
    def list_workflows() -> WorkflowListResponse:
        """Every workflow the orchestrator knows, newest first."""
        items = []
        for workflow_id in catalog.ids():
            try:
                state = engine.get_workflow_status(workflow_id)
            except WorkflowNotFoundError:
                # Its state is gone: drop it.
                catalog.forget(workflow_id)
                continue
            blueprint = catalog.blueprint(workflow_id) or {}
            items.append(
                WorkflowSummaryResponse(
                    workflow_id=workflow_id,
                    name=blueprint.get("name"),
                    status=state.status.value,
                    created_at=state.created_at,
                    updated_at=state.updated_at,
                    error=state.error,
                )
            )
        items.sort(key=lambda item: item.created_at, reverse=True)
        return WorkflowListResponse(workflows=items, total=len(items))

    @router.get(
        "/workflows/{workflow_id}/graph",
        response_model=WorkflowGraphResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Workflow graph with live status",
    )
    def get_workflow_graph(workflow_id: str) -> WorkflowGraphResponse:
        """Nodes and edges from the stored blueprint, with each node's task
        status and the health of the service it runs on."""
        try:
            state = engine.get_workflow_status(workflow_id)
            tasks = engine.get_all_tasks(workflow_id)
        except WorkflowNotFoundError:
            raise HTTPException(status_code=404, detail="Workflow not found")

        blueprint = catalog.blueprint(workflow_id)
        if blueprint is None:
            raise HTTPException(status_code=404, detail="Blueprint not available")

        status_by_key = {task.node_key: task.status.value for task in tasks}
        nodes, edges = _graph_nodes(blueprint)

        health_by_service = {}
        for node in nodes:
            service = node["container_name"]
            if service not in health_by_service:
                health_by_service[service] = registry.get_health(service)

        return WorkflowGraphResponse(
            workflow_id=workflow_id,
            name=blueprint.get("name"),
            status=state.status.value,
            nodes=[
                GraphNodeResponse(
                    **node,
                    status=status_by_key.get(node["key"], "pending"),
                    service_status=getattr(health_by_service[node["container_name"]], "status", None),
                    circuit_state=getattr(health_by_service[node["container_name"]], "circuit_state", None),
                )
                for node in nodes
            ],
            edges=[GraphEdgeResponse(source=s, target=t) for s, t in edges],
        )

    @router.get(
        "/workflows/{workflow_id}/logs",
        response_class=PlainTextResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Log lines mentioning a workflow",
    )
    def get_workflow_logs(
        workflow_id: str, lines: int = Query(200, ge=1, le=MAX_LINES)
    ) -> str:
        """Merged, redacted log lines from the API and every worker."""
        try:
            engine.get_workflow_status(workflow_id)
        except WorkflowNotFoundError:
            raise HTTPException(status_code=404, detail="Workflow not found")

        if reader.directory() is None:
            return "(orchestrator log directory not found)"
        records, matched = reader.workflow_log(workflow_id, lines)
        header = "" if matched else "(no lines mention this workflow; showing recent log)\n"
        return header + "\n".join(records)

    @router.get(
        "/orchestrator/alerts",
        response_model=AlertListResponse,
        summary="Recent alerts from all processes",
    )
    def get_alerts(limit: int = Query(50, ge=1, le=MAX_LINES)) -> AlertListResponse:
        """SERVICE_DOWN / RECOVERED / MANUAL_RECOVERY alerts, newest first."""
        alerts = reader.alerts(limit)
        return AlertListResponse(alerts=alerts, total=len(alerts))

    return router
