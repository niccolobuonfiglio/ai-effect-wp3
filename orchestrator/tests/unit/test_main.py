"""Unit tests for main entry point."""

import os
from unittest.mock import MagicMock, patch

import pytest

from main import create_app, get_redis_client, main


class TestGetRedisClient:
    """Tests for get_redis_client."""

    def test_uses_default_url(self):
        """Should use default localhost URL."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("main.redis.Redis") as mock_redis:
                get_redis_client()
                mock_redis.from_url.assert_called_once_with(
                    "redis://localhost:6379", decode_responses=True
                )

    def test_uses_environment_url(self):
        """Should use REDIS_URL from environment."""
        with patch.dict(os.environ, {"REDIS_URL": "redis://custom:1234"}):
            with patch("main.redis.Redis") as mock_redis:
                get_redis_client()
                mock_redis.from_url.assert_called_once_with(
                    "redis://custom:1234", decode_responses=True
                )


class TestCreateApp:
    """Tests for create_app."""

    def test_creates_fastapi_app(self):
        """Should create FastAPI application with dependencies."""
        mock_redis = MagicMock()

        with patch("main.get_redis_client", return_value=mock_redis):
            with patch("main.RedisStateStore") as mock_state_store:
                with patch("main.RedisTaskQueue") as mock_task_queue:
                    with patch("main.WorkflowEngine") as mock_engine:
                        with patch("main.BlueprintParser") as mock_bp:
                            with patch("main.DockerInfoParser") as mock_dp:
                                with patch("main.OrchestratorAPI") as mock_api:
                                    mock_app = MagicMock()
                                    mock_api.return_value.create_app.return_value = mock_app

                                    result = create_app()

                                    assert result == mock_app
                                    mock_state_store.assert_called_once_with(mock_redis)
                                    mock_task_queue.assert_called_once_with(mock_redis)
                                    mock_engine.assert_called_once()
                                    mock_bp.assert_called_once()
                                    mock_dp.assert_called_once()
                                    mock_api.assert_called_once()


class TestMain:
    """Tests for main function."""

    def test_parses_arguments(self):
        """Should parse command line arguments."""
        mock_app = MagicMock()

        with patch("main.create_app", return_value=mock_app):
            with patch("main.uvicorn.run") as mock_uvicorn:
                with patch("sys.argv", ["main.py", "--host", "127.0.0.1", "--port", "9000"]):
                    result = main()

                    assert result == 0
                    mock_uvicorn.assert_called_once()
                    call_kwargs = mock_uvicorn.call_args
                    assert call_kwargs[1]["host"] == "127.0.0.1"
                    assert call_kwargs[1]["port"] == 9000

    def test_uses_default_values(self):
        """Should use default host and port."""
        mock_app = MagicMock()

        with patch.dict(os.environ, {}, clear=True):
            with patch("main.create_app", return_value=mock_app):
                with patch("main.uvicorn.run") as mock_uvicorn:
                    with patch("sys.argv", ["main.py"]):
                        main()

                        call_kwargs = mock_uvicorn.call_args
                        assert call_kwargs[1]["host"] == "0.0.0.0"
                        assert call_kwargs[1]["port"] == 8000

    def test_uses_environment_values(self):
        """Should use HOST and PORT from environment."""
        mock_app = MagicMock()

        with patch.dict(os.environ, {"HOST": "10.0.0.1", "PORT": "3000"}):
            with patch("main.create_app", return_value=mock_app):
                with patch("main.uvicorn.run") as mock_uvicorn:
                    with patch("sys.argv", ["main.py"]):
                        main()

                        call_kwargs = mock_uvicorn.call_args
                        assert call_kwargs[1]["host"] == "10.0.0.1"
                        assert call_kwargs[1]["port"] == 3000

    def test_sets_log_level(self):
        """Should set log level from argument."""
        mock_app = MagicMock()

        with patch("main.create_app", return_value=mock_app):
            with patch("main.uvicorn.run") as mock_uvicorn:
                with patch("sys.argv", ["main.py", "--log-level", "debug"]):
                    main()

                    call_kwargs = mock_uvicorn.call_args
                    assert call_kwargs[1]["log_level"] == "debug"
