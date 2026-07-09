"""Main entry point for the orchestrator API server."""

import argparse
import logging
import os
import sys

import redis
import uvicorn

from api.app import OrchestratorAPI
from services.blueprint_parser import BlueprintParser
from services.dockerinfo_parser import DockerInfoParser
from services.log_service import configure_logging
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)


def get_redis_client() -> redis.Redis:
    """Create Redis client from environment."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def create_app() -> "uvicorn.ASGIApplication":
    """Create FastAPI application with all dependencies."""
    redis_client = get_redis_client()
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    engine = WorkflowEngine(state_store, task_queue, redis_client)
    blueprint_parser = BlueprintParser()
    dockerinfo_parser = DockerInfoParser()

    api = OrchestratorAPI(engine, blueprint_parser, dockerinfo_parser, redis_client)
    return api.create_app()


def main() -> int:
    """Run the orchestrator API server."""
    parser = argparse.ArgumentParser(description="Orchestrator API Server")
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "0.0.0.0"),
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", "8000")),
        help="Port to bind to (default: 8000)",
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default=os.environ.get("LOG_LEVEL", "info").lower(),
        help="Log level (default: info)",
    )
    args = parser.parse_args()

    # Configure logging with file rotation
    configure_logging(
        log_dir="logs",
        log_file="orchestrator.log",
        level=getattr(logging, args.log_level.upper()),
    )

    logger.info("Starting orchestrator API server")
    logger.info(f"Redis: {os.environ.get('REDIS_URL', 'redis://localhost:6379')}")

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def get_app() -> "uvicorn.ASGIApplication":
    """Get or create the FastAPI application (for uvicorn import)."""
    return create_app()


if __name__ == "__main__":
    sys.exit(main())
