"""Test service implementing control interface for integration tests."""

import threading
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class ExecuteRequest(BaseModel):
    """Execute request body."""

    method: str
    workflow_id: str
    task_id: str
    inputs: list[dict] = []
    parameters: dict = {}


class DataRef(BaseModel):
    """Simplified data reference."""

    protocol: str
    uri: str
    format: str


# In-memory task store for async tasks
_tasks: dict[str, dict[str, Any]] = {}


def create_test_app() -> FastAPI:
    """Create test FastAPI application."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> dict:
        """Execute a task."""
        # Test method: simulates quick operation that completes immediately
        if request.method == "_test_quick":
            return {
                "status": "complete",
                "output": {
                    "protocol": "s3",
                    "uri": f"s3://bucket/output/{request.task_id}.json",
                    "format": "json",
                },
            }

        # Test method: simulates long-running operation requiring polling
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

        # Test method: simulates operation that fails
        if request.method == "_test_failing":
            return {
                "status": "failed",
                "error": "Task execution failed",
            }

        # Unknown method
        raise HTTPException(status_code=400, detail=f"Unknown method: {request.method}")

    @app.get("/control/status/{task_id}")
    def get_status(task_id: str) -> dict:
        """Get task status."""
        if task_id not in _tasks:
            raise HTTPException(status_code=404, detail="Task not found")

        task = _tasks[task_id]

        # Simulate progress
        if task["status"] == "running":
            task["progress"] = min(task["progress"] + 50, 100)
            if task["progress"] >= 100:
                task["status"] = "complete"

        return {
            "status": task["status"],
            "progress": task["progress"],
        }

    @app.get("/control/output/{task_id}")
    def get_output(task_id: str) -> dict:
        """Get task output."""
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

    @app.post("/reset")
    def reset() -> dict:
        """Reset task store for testing."""
        _tasks.clear()
        return {"status": "ok"}

    @app.get("/tasks/{task_id}")
    def get_task_info(task_id: str) -> dict:
        """Get task info for testing verification."""
        if task_id not in _tasks:
            raise HTTPException(status_code=404, detail="Task not found")
        return _tasks[task_id]

    return app


class ControlServiceRunner:
    """Runs control service in background thread for testing."""

    def __init__(self, host: str = "127.0.0.1", port: int = 18080):
        self.host = host
        self.port = port
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        """Start the test service."""
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
