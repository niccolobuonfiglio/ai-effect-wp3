"""HTTP client for service control endpoints."""

from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, field_validator

from models.data_reference import DataReference


class ControlClientError(Exception):
    """Raised when control call fails.

    ``transient`` is True when the service could not be reached or answered
    with a 5xx - i.e. the failure says something about service availability
    rather than about the request.
    """

    def __init__(self, message: str, transient: bool = False):
        super().__init__(message)
        self.transient = transient

    pass


class ExecuteRequest(BaseModel):
    """Request body for /control/execute."""

    model_config = ConfigDict(frozen=True)

    method: str
    workflow_id: str
    task_id: str
    inputs: list[DataReference] = []
    parameters: dict = {}

    @field_validator("method")
    @classmethod
    def method_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("method is required")
        return v

    @field_validator("workflow_id")
    @classmethod
    def workflow_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("workflow_id is required")
        return v

    @field_validator("task_id")
    @classmethod
    def task_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("task_id is required")
        return v


class ExecuteResponse(BaseModel):
    """Response from /control/execute."""

    model_config = ConfigDict(frozen=True)

    status: Literal["complete", "running", "failed"]
    task_id: str | None = None
    output: DataReference | None = None
    error: str | None = None


class StatusResponse(BaseModel):
    """Response from /control/status."""

    model_config = ConfigDict(frozen=True)

    status: Literal["running", "complete", "failed"]
    progress: int | None = None
    error: str | None = None


class OutputResponse(BaseModel):
    """Response from /control/output."""

    model_config = ConfigDict(frozen=True)

    output: DataReference


class ControlClient:
    """HTTP client for service control endpoints."""

    def __init__(
        self,
        timeout: float = 30.0,
        api_key: str | None = None,
        verify: bool | str = True,
    ):
        """Initialize client with timeout and optional bearer token.

        Args:
            timeout: Per-request timeout in seconds.
            api_key: Bearer token sent to the services.
            verify: TLS verification for https endpoints - True, False, or the
                path to a CA bundle.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._timeout = timeout
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._verify = verify

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
            with httpx.Client(timeout=self._timeout, verify=self._verify) as client:
                response = client.post(url, json=request.model_dump(mode="json"), headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}", transient=True) from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}", transient=True) from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}", transient=True) from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}",
                transient=response.status_code >= 500,
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
            with httpx.Client(timeout=self._timeout, verify=self._verify) as client:
                response = client.get(url, headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}", transient=True) from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}", transient=True) from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}", transient=True) from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}",
                transient=response.status_code >= 500,
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
            with httpx.Client(timeout=self._timeout, verify=self._verify) as client:
                response = client.get(url, headers=self._headers)
        except httpx.ConnectError as e:
            raise ControlClientError(f"Connection failed: {e}", transient=True) from e
        except httpx.TimeoutException as e:
            raise ControlClientError(f"Request timed out: {e}", transient=True) from e
        except httpx.RequestError as e:
            raise ControlClientError(f"Request failed: {e}", transient=True) from e

        if response.status_code >= 400:
            raise ControlClientError(
                f"HTTP {response.status_code}: {response.text}",
                transient=response.status_code >= 500,
            )

        try:
            data = response.json()
            return OutputResponse.model_validate(data)
        except Exception as e:
            raise ControlClientError(f"Invalid response: {e}") from e
