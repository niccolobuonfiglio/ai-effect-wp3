"""Unit tests for NodeValidator.

These tests use ``pytest-httpx`` to stand in for a real service and exercise
the validator against the Node Contract without any network or Docker. They
cover the three required scenarios — a healthy conformant service, a service
missing ``/health``, and a service returning a malformed ``/control/execute``
body — plus transport-level failures and the constructor guard.
"""

import httpx
import pytest
from pytest_httpx import HTTPXMock

from models.data_reference import DataReference, Format, Protocol
from services.node_validator import (
    DEFAULT_PROBE_METHOD,
    NodeValidator,
    ValidationResult,
)

BASE_URL = "http://service:8080"
HEALTH_URL = f"{BASE_URL}/health"
EXECUTE_URL = f"{BASE_URL}/control/execute"


@pytest.fixture
def validator() -> NodeValidator:
    return NodeValidator(timeout=5.0)


@pytest.fixture
def valid_execute_body() -> dict:
    """A well-formed ExecuteResponse body (synchronous completion)."""
    output = DataReference(
        protocol=Protocol.S3,
        uri="s3://bucket/output/_validate.json",
        format=Format.JSON,
    )
    return {"status": "complete", "output": output.model_dump(mode="json")}


def _check(result: ValidationResult, name: str):
    """Return the CheckResult with the given name, or fail the test."""
    for c in result.checks:
        if c.name == name:
            return c
    pytest.fail(f"no check named {name!r} in result: {result.checks}")


class TestNodeValidatorInit:
    """Constructor validation."""

    def test_default_timeout(self):
        assert NodeValidator()._timeout == 10.0

    def test_custom_timeout(self):
        assert NodeValidator(timeout=3.0)._timeout == 3.0

    def test_zero_timeout_raises(self):
        with pytest.raises(ValueError, match="timeout must be positive"):
            NodeValidator(timeout=0)

    def test_negative_timeout_raises(self):
        with pytest.raises(ValueError, match="timeout must be positive"):
            NodeValidator(timeout=-2.0)


class TestValidateArguments:
    """Argument guards on validate()."""

    def test_empty_base_url_raises(self, validator):
        with pytest.raises(ValueError, match="base_url is required"):
            validator.validate("")

    def test_whitespace_base_url_raises(self, validator):
        with pytest.raises(ValueError, match="base_url is required"):
            validator.validate("   ")


class TestHealthyService:
    """A fully conformant service passes every check."""

    def test_healthy_service_succeeds(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        result = validator.validate(BASE_URL)

        assert result.success is True
        assert result.errors == []
        assert _check(result, "health").passed is True
        assert _check(result, "control_execute").passed is True

    def test_healthy_service_probes_with_default_method(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        """The execute probe uses the reference service's quick method by default."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        validator.validate(BASE_URL)

        execute_request = [
            r for r in httpx_mock.get_requests() if r.url == EXECUTE_URL
        ][0]
        import json

        body = json.loads(execute_request.content)
        assert body["method"] == DEFAULT_PROBE_METHOD

    def test_running_status_is_valid(self, validator, httpx_mock: HTTPXMock):
        """A ``running`` ExecuteResponse (async start) is contract-valid."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST",
            url=EXECUTE_URL,
            json={"status": "running", "task_id": "svc-1"},
        )

        result = validator.validate(BASE_URL)

        assert result.success is True

    def test_trailing_slash_base_url(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        """A trailing slash on base_url does not produce doubled slashes."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        result = validator.validate(f"{BASE_URL}/")

        assert result.success is True


class TestMissingHealth:
    """Health-endpoint problems fail the health check but still probe execute."""

    def test_health_404_fails(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=404)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        health = _check(result, "health")
        assert health.passed is False
        assert "404" in health.detail
        # The execute check is independent and still ran and passed.
        assert _check(result, "control_execute").passed is True
        assert any("health" in e for e in result.errors)

    def test_health_500_fails(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=500)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        assert _check(result, "health").passed is False

    def test_health_connection_error_fails(
        self, validator, httpx_mock: HTTPXMock, valid_execute_body
    ):
        """A transport error reaching /health is a failed check, not an exception."""
        httpx_mock.add_exception(
            httpx.ConnectError("connection refused"),
            method="GET",
            url=HEALTH_URL,
        )
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json=valid_execute_body
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        health = _check(result, "health")
        assert health.passed is False
        assert "failed" in health.detail.lower()


class TestMalformedExecute:
    """/control/execute bodies that violate the contract fail that check."""

    def test_malformed_json_object_fails(self, validator, httpx_mock: HTTPXMock):
        """A 200 body that isn't a valid ExecuteResponse fails validation."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST",
            url=EXECUTE_URL,
            json={"unexpected": "shape", "foo": 123},
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        execute = _check(result, "control_execute")
        assert execute.passed is False
        assert "ExecuteResponse" in execute.detail

    def test_invalid_status_enum_fails(self, validator, httpx_mock: HTTPXMock):
        """A status outside the allowed literal set is rejected."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST",
            url=EXECUTE_URL,
            json={"status": "banana"},
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        assert _check(result, "control_execute").passed is False

    def test_non_json_body_fails(self, validator, httpx_mock: HTTPXMock):
        """A non-JSON 200 body is reported as a JSON parse failure."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST",
            url=EXECUTE_URL,
            content=b"this is not json",
            headers={"Content-Type": "text/plain"},
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        execute = _check(result, "control_execute")
        assert execute.passed is False
        assert "json" in execute.detail.lower()

    def test_execute_http_error_fails(self, validator, httpx_mock: HTTPXMock):
        """A 400+ from /control/execute fails the execute check."""
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_response(
            method="POST",
            url=EXECUTE_URL,
            status_code=500,
            text="boom",
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        assert _check(result, "control_execute").passed is False

    def test_execute_connection_error_fails(self, validator, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=200)
        httpx_mock.add_exception(
            httpx.ConnectError("connection refused"),
            method="POST",
            url=EXECUTE_URL,
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        assert _check(result, "control_execute").passed is False


class TestBothEndpointsBroken:
    """When everything is broken, both checks fail and both errors surface."""

    def test_all_failures_reported(self, validator, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="GET", url=HEALTH_URL, status_code=503)
        httpx_mock.add_response(
            method="POST", url=EXECUTE_URL, json={"nope": True}
        )

        result = validator.validate(BASE_URL)

        assert result.success is False
        assert _check(result, "health").passed is False
        assert _check(result, "control_execute").passed is False
        assert len(result.errors) == 2