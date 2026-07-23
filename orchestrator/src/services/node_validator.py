"""Validate that a service conforms to the Node Contract.

:class:`NodeValidator` performs black-box conformance checks against a running
service's HTTP endpoints and reports the outcome as a structured
:class:`ValidationResult`. It never raises for a non-conformant service — a
service being wrong is an expected outcome, captured in the result — and only
surfaces programming errors (e.g. an empty ``base_url``) as exceptions.

-------------
Two checks are implemented, both validated against the models in
:mod:`services.contract` (the single source of truth):

* ``GET /health`` returns HTTP 200.
* ``POST /control/execute`` returns a body whose structure matches
  :class:`~services.contract.ExecuteResponse`.

The ``/control/execute`` probe uses a caller-supplied ``method`` (defaulting to
the reference service's ``_test_quick``) so it can drive the target's
synchronous-completion path without side effects. Deeper status/output polling
checks are intentionally out of scope for Phase 1 and can be layered on later
without changing this result shape.
"""

from __future__ import annotations

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from services.contract import ExecuteResponse

# Default method used to probe /control/execute. Matches the "quick completion"
# pattern implemented by the gold-standard reference service
# (tests/fixtures/test_service.py), which returns status="complete" immediately.
DEFAULT_PROBE_METHOD = "_test_quick"


class CheckResult(BaseModel):
    """Outcome of a single conformance check."""

    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool
    detail: str = ""


class ValidationResult(BaseModel):
    """Structured result of validating a service against the Node Contract.

    ``success`` is the overall verdict (true only when every check passed).
    ``checks`` holds the per-check breakdown, and ``errors`` is the flat list
    of human-readable messages for the checks that failed — convenient for
    logging or surfacing to a user without walking ``checks``.
    """

    model_config = ConfigDict(frozen=True)

    base_url: str
    success: bool
    checks: list[CheckResult] = []
    errors: list[str] = []


class NodeValidator:
    """Checks a running service against the Node Contract.

    The validator is stateless apart from its timeout; a single instance can
    validate many services. All HTTP failures are converted into failed
    checks rather than propagating, so callers can rely on always receiving a
    :class:`ValidationResult`.
    """

    def __init__(self, timeout: float = 10.0):
        """Initialize the validator.

        Args:
            timeout: Per-request HTTP timeout in seconds. Must be positive.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._timeout = timeout

    def validate(
        self,
        base_url: str,
        probe_method: str = DEFAULT_PROBE_METHOD,
        workflow_id: str = "_validate",
        task_id: str = "_validate",
    ) -> ValidationResult:
        """Validate a service and return a structured result.

        Runs every check (they do not short-circuit) so the returned result
        describes the full conformance picture in one pass.

        Args:
            base_url: Base URL of the service, e.g. ``http://host:8080``.
            probe_method: ``method`` sent to ``/control/execute``. Defaults to
                the reference service's synchronous ``_test_quick`` operation.
            workflow_id: Workflow id sent in the probe request body.
            task_id: Task id sent in the probe request body.

        Returns:
            A :class:`ValidationResult`. ``success`` is true only if all
            checks passed.

        Raises:
            ValueError: If ``base_url`` is empty.
        """
        if not base_url or not base_url.strip():
            raise ValueError("base_url is required")

        root = base_url.rstrip("/")
        checks: list[CheckResult] = [
            self._check_health(root),
            self._check_execute(root, probe_method, workflow_id, task_id),
        ]

        errors = [f"{c.name}: {c.detail}" for c in checks if not c.passed]
        return ValidationResult(
            base_url=base_url,
            success=all(c.passed for c in checks),
            checks=checks,
            errors=errors,
        )

    def _check_health(self, root: str) -> CheckResult:
        """Check that ``GET /health`` returns HTTP 200."""
        url = f"{root}/health"
        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url)
        except httpx.HTTPError as e:
            return CheckResult(
                name="health",
                passed=False,
                detail=f"request to {url} failed: {e}",
            )

        if response.status_code != 200:
            return CheckResult(
                name="health",
                passed=False,
                detail=f"expected HTTP 200 from {url}, got {response.status_code}",
            )

        return CheckResult(name="health", passed=True, detail="200 OK")

    def _check_execute(
        self,
        root: str,
        method: str,
        workflow_id: str,
        task_id: str,
    ) -> CheckResult:
        """Check that ``POST /control/execute`` matches :class:`ExecuteResponse`."""
        url = f"{root}/control/execute"
        payload = {
            "method": method,
            "workflow_id": workflow_id,
            "task_id": task_id,
            "inputs": [],
            "parameters": {},
        }

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(url, json=payload)
        except httpx.HTTPError as e:
            return CheckResult(
                name="control_execute",
                passed=False,
                detail=f"request to {url} failed: {e}",
            )

        if response.status_code >= 400:
            return CheckResult(
                name="control_execute",
                passed=False,
                detail=(
                    f"expected a success status from {url}, "
                    f"got HTTP {response.status_code}: {response.text}"
                ),
            )

        try:
            data = response.json()
        except ValueError as e:
            return CheckResult(
                name="control_execute",
                passed=False,
                detail=f"response body from {url} is not valid JSON: {e}",
            )

        try:
            ExecuteResponse.model_validate(data)
        except ValidationError as e:
            return CheckResult(
                name="control_execute",
                passed=False,
                detail=f"response does not match ExecuteResponse contract: {e}",
            )

        return CheckResult(
            name="control_execute",
            passed=True,
            detail="response matches ExecuteResponse",
        )


__all__ = [
    "CheckResult",
    "ValidationResult",
    "NodeValidator",
    "DEFAULT_PROBE_METHOD",
]