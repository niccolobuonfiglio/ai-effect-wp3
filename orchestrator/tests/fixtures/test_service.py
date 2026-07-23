"""Gold-standard reference implementation of the Node Contract.

============================================================================
GOLD STANDARD REFERENCE SERVICE
============================================================================
This module is the example of a service ("node") that correctly speaks the 
AI-Effect Node Contract. It is used as the fixture for integration tests, 
and it doubles as executable documentation: any new pipeline service can be 
checked for conformance against the behavior shown here, and 
:class:`services.node_validator.NodeValidator` validates real services 
against the very same contract this fixture satisfies.

The narrative walk-through of the three execution patterns lives in
``docs/service-implementation-patterns.md``.

----------------------------------------------------------------------------
THE FOUR MANDATORY ENDPOINTS
----------------------------------------------------------------------------
1. GET  /health                     -> 200 OK when the service is ready.
2. POST /control/execute            -> ExecuteResponse. Starts work; either
                                       completes synchronously or returns a
                                       task_id to poll.
3. GET  /control/status/{task_id}   -> StatusResponse. Progress of an async
                                       task started by /control/execute.
4. GET  /control/output/{task_id}   -> OutputResponse. Result of a completed
                                       async task.

The request/response shapes are defined once in ``services.contract`` and are
the single source of truth. This fixture intentionally re-declares only the
tiny subset of those shapes it needs for the request body, to stay dependency-
light and self-contained as a reference.

----------------------------------------------------------------------------
THE THREE EXECUTION PATTERNS (see /control/execute below)
----------------------------------------------------------------------------
* ``_test_quick``        -> quick completion: returns status="complete" with
                            an output immediately (no polling required).
* ``_test_long_running`` -> long-running async: returns status="running" with
                            a task_id; the orchestrator then polls
                            /control/status until complete and fetches
                            /control/output.
* ``_test_failing``      -> failure: returns status="failed" with an error.

----------------------------------------------------------------------------
TEST-ONLY HELPERS (NOT part of the contract)
----------------------------------------------------------------------------
``POST /reset`` and ``GET /tasks/{task_id}`` exist purely so tests can reset
state between cases and inspect internal task bookkeeping. Real services need
not implement them, and they are clearly marked below.
============================================================================
"""

import threading
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class ExecuteRequest(BaseModel):
    """Execute request body.

    Mirrors the subset of ``services.contract.ExecuteRequest`` that this
    reference service reads. Kept local so the fixture stays self-contained.
    """

    method: str
    workflow_id: str
    task_id: str
    inputs: list[dict] = []
    parameters: dict = {}


class DataRef(BaseModel):
    """Simplified data reference (mirrors the fields of models.DataReference)."""

    protocol: str
    uri: str
    format: str


# In-memory task store for async tasks (keyed by service-side task id).
_tasks: dict[str, dict[str, Any]] = {}


def create_test_app() -> FastAPI:
    """Create the gold-standard reference FastAPI application.

    The returned app implements all four mandatory Node Contract endpoints
    plus two clearly-marked test-only helpers.
    """
    app = FastAPI()

    # ---------------------------------------------------------------------
    # MANDATORY ENDPOINT 1/4: GET /health
    # ---------------------------------------------------------------------
    # Returns 200 OK to signal the service is up and ready to accept work.
    # NodeValidator treats any non-200 (or unreachable) /health as a failure.
    @app.get("/health")
    def health() -> dict:
        """Health check: 200 OK when the service is ready."""
        return {"status": "ok"}

    # ---------------------------------------------------------------------
    # MANDATORY ENDPOINT 2/4: POST /control/execute
    # ---------------------------------------------------------------------
    # Entry point for all work. Demonstrates the three execution patterns via
    # the request ``method``. The response always conforms to
    # services.contract.ExecuteResponse.
    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> dict:
        """Execute a task (dispatches on ``method`` to one of three patterns)."""
        # PATTERN 1 -- QUICK COMPLETION.
        # Work is trivial/synchronous: return the result immediately with
        # status="complete". No task_id, no polling.
        if request.method == "_test_quick":
            return {
                "status": "complete",
                "output": {
                    "protocol": "s3",
                    "uri": f"s3://bucket/output/{request.task_id}.json",
                    "format": "json",
                },
            }

        # PATTERN 2 -- LONG-RUNNING ASYNC.
        # Work will take a while: register a service-side task, return
        # status="running" plus a task_id. The orchestrator polls
        # /control/status/{task_id} and then reads /control/output/{task_id}.
        if request.method == "_test_long_running":
            service_task_id = f"svc-{request.task_id}"
            _tasks[service_task_id] = {
                "status": "running",
                "progress": 0,
                "request": request.model_dump(),
            }
            return {
                "status": "running",
                "task_id": service_task_id,
            }

        # PATTERN 3 -- FAILURE.
        # Work cannot be performed: return status="failed" with an error
        # message the orchestrator will record on the task.
        if request.method == "_test_failing":
            return {
                "status": "failed",
                "error": "Task execution failed",
            }

        # Unknown method -> 400. (A real service would dispatch to its own
        # operations here.)
        raise HTTPException(status_code=400, detail=f"Unknown method: {request.method}")

    # ---------------------------------------------------------------------
    # MANDATORY ENDPOINT 3/4: GET /control/status/{task_id}
    # ---------------------------------------------------------------------
    # Reports progress of an async task started by PATTERN 2. Response
    # conforms to services.contract.StatusResponse.
    @app.get("/control/status/{task_id}")
    def get_status(task_id: str) -> dict:
        """Get async task status (drives orchestrator polling)."""
        if task_id not in _tasks:
            raise HTTPException(status_code=404, detail="Task not found")

        task = _tasks[task_id]

        # Simulate forward progress so repeated polls eventually complete.
        if task["status"] == "running":
            task["progress"] = min(task["progress"] + 50, 100)
            if task["progress"] >= 100:
                task["status"] = "complete"

        return {
            "status": task["status"],
            "progress": task["progress"],
        }

    # ---------------------------------------------------------------------
    # MANDATORY ENDPOINT 4/4: GET /control/output/{task_id}
    # ---------------------------------------------------------------------
    # Returns the result of a completed async task. Response conforms to
    # services.contract.OutputResponse. Fetched only after /control/status
    # reports "complete".
    @app.get("/control/output/{task_id}")
    def get_output(task_id: str) -> dict:
        """Get async task output (fetched once status is complete)."""
        if task_id not in _tasks:
            raise HTTPException(status_code=404, detail="Task not found")

        task = _tasks[task_id]
        if task["status"] != "complete":
            raise HTTPException(status_code=400, detail="Task not complete")

        return {
            "output": {
                "protocol": "s3",
                "uri": f"s3://bucket/output/{task_id}.json",
                "format": "json",
            },
        }

    # =====================================================================
    # TEST-ONLY HELPERS BELOW -- NOT part of the Node Contract.
    # Real services do not need these; they exist so tests can reset state
    # and inspect internal task bookkeeping.
    # =====================================================================
    @app.post("/reset")
    def reset() -> dict:
        """TEST-ONLY: reset the in-memory task store between test cases."""
        _tasks.clear()
        return {"status": "ok"}

    @app.get("/tasks/{task_id}")
    def get_task_info(task_id: str) -> dict:
        """TEST-ONLY: expose raw task bookkeeping for test assertions."""
        if task_id not in _tasks:
            raise HTTPException(status_code=404, detail="Task not found")
        return _tasks[task_id]

    return app


class ControlServiceRunner:
    """Runs the gold-standard reference service in a background thread.

    Used by integration tests to bring up a real HTTP server implementing the
    Node Contract, then tear it down cleanly.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 18080):
        self.host = host
        self.port = port
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        """Start the test service and block until it is ready."""
        app = create_test_app()
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="error",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

        # Wait for server to be ready
        self._wait_for_ready()

    def _wait_for_ready(self, timeout: float = 5.0) -> None:
        """Wait for server to be ready."""
        import httpx

        start = time.time()
        while time.time() - start < timeout:
            try:
                with httpx.Client() as client:
                    client.post(f"{self.base_url}/reset")
                    return
            except httpx.ConnectError:
                time.sleep(0.1)
        raise RuntimeError("Test service did not start in time")

    def stop(self) -> None:
        """Stop the test service."""
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=2.0)

    def reset(self) -> None:
        """Reset task store."""
        import httpx

        with httpx.Client() as client:
            client.post(f"{self.base_url}/reset")