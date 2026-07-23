"""HTTP client for service control endpoints.

The request/response contract models (:class:`ExecuteRequest`,
:class:`ExecuteResponse`, :class:`StatusResponse`, :class:`OutputResponse`)
now live in :mod:`services.contract`, the single source of truth for the Node
Contract. They are re-exported here so that existing imports such as
``from services.control_client import ExecuteResponse`` continue to work
unchanged. New code should prefer importing them from ``services.contract``.
"""

import httpx

from models.data_reference import DataReference
from services.contract import (
    ExecuteRequest,
    ExecuteResponse,
    OutputResponse,
    StatusResponse,
)

__all__ = [
    "ControlClient",
    "ControlClientError",
    "ExecuteRequest",
    "ExecuteResponse",
    "OutputResponse",
    "StatusResponse",
]


class ControlClientError(Exception):
    """Raised when control call fails."""

    pass


class ControlClient:
    """HTTP client for service control endpoints."""

    def __init__(self, timeout: float = 30.0, api_key: str | None = None):
        """Initialize client with timeout and optional bearer token."""
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._timeout = timeout
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    def execute(
        self,
        base_url: str,
        method: str,
        workflow_id: str,
        task_id: str,
        inputs: list[DataReference] | None = None,
        parameters: dict | None = None,
    ) -> ExecuteResponse:
        """Call POST /control/execute."""
        if not base_url or not base_url.strip():
            raise ValueError("base_url is required")

        request = ExecuteRequest(
            method=method,
            workflow_id=workflow_id,
            task_id=task_id,
            inputs=inputs or [],
            parameters=parameters or {},
        )

        url = f"{base_url.rstrip('/')}/control/execute"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(url, json=request.model_dump(mode="json"), headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}") from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}") from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}") from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
            return ExecuteResponse.model_validate(data)
        except Exception as e:
            raise ControlClientError(f"Invalid response: {e}") from e

    def get_status(self, base_url: str, task_id: str) -> StatusResponse:
        """Call GET /control/status/{task_id}."""
        if not base_url or not base_url.strip():
            raise ValueError("base_url is required")
        if not task_id or not task_id.strip():
            raise ValueError("task_id is required")

        url = f"{base_url.rstrip('/')}/control/status/{task_id}"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}") from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}") from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}") from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
            return StatusResponse.model_validate(data)
        except Exception as e:
            raise ControlClientError(f"Invalid response: {e}") from e

    def get_output(self, base_url: str, task_id: str) -> OutputResponse:
        """Call GET /control/output/{task_id}."""
        if not base_url or not base_url.strip():
            raise ValueError("base_url is required")
        if not task_id or not task_id.strip():
            raise ValueError("task_id is required")

        url = f"{base_url.rstrip('/')}/control/output/{task_id}"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}") from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}") from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}") from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
            return OutputResponse.model_validate(data)
        except Exception as e:
            raise ControlClientError(f"Invalid response: {e}") from e