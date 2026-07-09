"""Unit tests for worker CLI."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from services.dockerinfo_parser import ServiceEndpoint
from worker_cli import get_redis_client, load_endpoints, main


class TestGetRedisClient:
    """Tests for get_redis_client."""

    def test_uses_default_url(self):
        """Should use default localhost URL."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("worker_cli.redis.Redis") as mock_redis:
                get_redis_client()
                mock_redis.from_url.assert_called_once_with(
                    "redis://localhost:6379", decode_responses=True
                )

    def test_uses_environment_url(self):
        """Should use REDIS_URL from environment."""
        with patch.dict(os.environ, {"REDIS_URL": "redis://custom:1234"}):
            with patch("worker_cli.redis.Redis") as mock_redis:
                get_redis_client()
                mock_redis.from_url.assert_called_once_with(
                    "redis://custom:1234", decode_responses=True
                )


class TestLoadEndpoints:
    """Tests for load_endpoints."""

    def test_loads_endpoints_from_redis(self):
        """Should load and parse endpoints from Redis hash."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {
            "service-a": json.dumps({
                "container_name": "service-a",
                "address": "localhost",
                "port": 8080,
            }),
            "service-b": json.dumps({
                "container_name": "service-b",
                "address": "localhost",
                "port": 8081,
            }),
        }

        endpoints = load_endpoints(mock_redis, "wf-123")

        assert len(endpoints) == 2
        assert "service-a" in endpoints
        assert "service-b" in endpoints
        assert endpoints["service-a"].address == "localhost"
        assert endpoints["service-a"].port == 8080
        mock_redis.hgetall.assert_called_once_with("endpoints:wf-123")

    def test_raises_when_no_endpoints(self):
        """Should raise ValueError when no endpoints found."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {}

        with pytest.raises(ValueError, match="No endpoints found"):
            load_endpoints(mock_redis, "wf-123")


class TestMain:
    """Tests for main function."""

    def test_runs_worker_for_workflow(self):
        """Should create worker and run for workflow."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {
            "service-a": json.dumps({
                "container_name": "service-a",
                "address": "localhost",
                "port": 8080,
            }),
        }

        with patch("worker_cli.get_redis_client", return_value=mock_redis):
            with patch("worker_cli.RedisStateStore"):
                with patch("worker_cli.RedisTaskQueue"):
                    with patch("worker_cli.WorkflowEngine"):
                        with patch("worker_cli.ControlClient"):
                            with patch("worker_cli.Worker") as mock_worker_cls:
                                mock_worker = MagicMock()
                                mock_worker_cls.return_value = mock_worker

                                with patch("sys.argv", ["worker_cli.py", "wf-123"]):
                                    result = main()

                                    assert result == 0
                                    mock_worker.run.assert_called_once_with(
                                        "wf-123", timeout=0
                                    )

    def test_uses_timeout_argument(self):
        """Should pass timeout to worker.run."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {
            "service-a": json.dumps({
                "container_name": "service-a",
                "address": "localhost",
                "port": 8080,
            }),
        }

        with patch("worker_cli.get_redis_client", return_value=mock_redis):
            with patch("worker_cli.RedisStateStore"):
                with patch("worker_cli.RedisTaskQueue"):
                    with patch("worker_cli.WorkflowEngine"):
                        with patch("worker_cli.ControlClient"):
                            with patch("worker_cli.Worker") as mock_worker_cls:
                                mock_worker = MagicMock()
                                mock_worker_cls.return_value = mock_worker

                                with patch(
                                    "sys.argv",
                                    ["worker_cli.py", "wf-123", "--timeout", "30"],
                                ):
                                    main()

                                    mock_worker.run.assert_called_once_with(
                                        "wf-123", timeout=30
                                    )

    def test_uses_poll_interval_argument(self):
        """Should pass poll_interval to Worker constructor."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {
            "service-a": json.dumps({
                "container_name": "service-a",
                "address": "localhost",
                "port": 8080,
            }),
        }

        with patch("worker_cli.get_redis_client", return_value=mock_redis):
            with patch("worker_cli.RedisStateStore"):
                with patch("worker_cli.RedisTaskQueue"):
                    with patch("worker_cli.WorkflowEngine") as mock_engine_cls:
                        with patch("worker_cli.ControlClient") as mock_client_cls:
                            with patch("worker_cli.Worker") as mock_worker_cls:
                                mock_worker = MagicMock()
                                mock_worker_cls.return_value = mock_worker

                                with patch(
                                    "sys.argv",
                                    ["worker_cli.py", "wf-123", "--poll-interval", "2.5"],
                                ):
                                    main()

                                    # Check poll_interval was passed to Worker
                                    call_kwargs = mock_worker_cls.call_args[1]
                                    assert call_kwargs["poll_interval"] == 2.5

    def test_fails_when_no_endpoints(self):
        """Should fail when workflow has no endpoints."""
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {}

        with patch("worker_cli.get_redis_client", return_value=mock_redis):
            with patch("sys.argv", ["worker_cli.py", "wf-123"]):
                with pytest.raises(ValueError, match="No endpoints found"):
                    main()
