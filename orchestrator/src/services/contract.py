"""Node Contract: the single source of truth for the service control interface.

Every AI-Effect pipeline service ("node") speaks the same small HTTP control
protocol so the orchestrator can drive it without knowing anything about what
the service does internally. This module defines that protocol's request and
response shapes as Pydantic models.

Node Contract standardization moved these models here out of
``control_client.py`` so that both the *client* (the orchestrator's
``ControlClient``) and the *validator* (``NodeValidator``) — and any future
tooling or reference service — validate against exactly one definition instead
of drifting copies.

The contract surface
--------------------
A conformant node MUST expose four endpoints:

* ``GET  /health``                      -> 200 OK when the service is ready
* ``POST /control/execute``             -> :class:`ExecuteResponse`
* ``GET  /control/status/{task_id}``    -> :class:`StatusResponse`
* ``GET  /control/output/{task_id}``    -> :class:`OutputResponse`

``/control/execute`` accepts an :class:`ExecuteRequest` body. It may either
complete synchronously (``status="complete"`` with an ``output``) or start an
asynchronous task (``status="running"`` with a ``task_id`` the orchestrator
then polls via ``/control/status`` and ``/control/output``).

Backward compatibility
-----------------------
``services.control_client`` re-exports every name defined here, so existing
imports such as ``from services.control_client import ExecuteResponse`` keep
working unchanged. New code should prefer importing from
``services.contract`` directly.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from models.data_reference import DataReference


class ExecuteRequest(BaseModel):
    """Request body for ``POST /control/execute``.

    Sent by the orchestrator to ask a node to run one operation (``method``)
    for a given workflow/task, optionally passing input data references and
    free-form parameters.
    """

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
    """Response from ``POST /control/execute``.

    ``status`` drives orchestrator behavior:

    * ``"complete"`` -- work finished synchronously; ``output`` carries the
      result (may be ``None`` for sink nodes that produce nothing).
    * ``"running"``  -- work started asynchronously; ``task_id`` is the
      service-side handle the orchestrator polls.
    * ``"failed"``   -- work could not be performed; ``error`` explains why.
    """

    model_config = ConfigDict(frozen=True)

    status: Literal["complete", "running", "failed"]
    task_id: str | None = None
    output: DataReference | None = None
    error: str | None = None


class StatusResponse(BaseModel):
    """Response from ``GET /control/status/{task_id}``.

    Polled by the orchestrator while an asynchronous task is in flight.
    ``progress`` is an optional 0-100 hint; ``error`` is populated when
    ``status="failed"``.
    """

    model_config = ConfigDict(frozen=True)

    status: Literal["running", "complete", "failed"]
    progress: int | None = None
    error: str | None = None


class OutputResponse(BaseModel):
    """Response from ``GET /control/output/{task_id}``.

    Returned once an asynchronous task has reached ``complete``; carries the
    single resulting :class:`DataReference`.
    """

    model_config = ConfigDict(frozen=True)

    output: DataReference


__all__ = [
    "ExecuteRequest",
    "ExecuteResponse",
    "StatusResponse",
    "OutputResponse",
]