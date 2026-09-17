"""Main entry point for the orchestrator API server."""

import argparse
import logging
import os
import sys

import redis
import uvicorn

from api.app import OrchestratorAPI
from services.blueprint_parser import BlueprintParser
from services.circuit_breaker import CircuitBreaker
from services.dockerinfo_parser import DockerInfoParser
from services.log_service import configure_logging
from services.service_registry import ServiceRegistry
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)


def get_redis_client() -> redis.Redis:
    """Create Redis client from environment."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def get_circuit_breaker(redis_client: redis.Redis) -> CircuitBreaker:
    """Create circuit breaker from environment.

    State lives in Redis, so the API process and every worker replica share a
    single view of service health.
    """
    return CircuitBreaker(
        redis_client,
        failure_threshold=int(os.environ.get("CIRCUIT_FAILURE_THRESHOLD", "3")),
        reset_timeout=float(os.environ.get("CIRCUIT_RESET_TIMEOUT", "60")),
        degraded_threshold=int(os.environ.get("CIRCUIT_DEGRADED_THRESHOLD", "1")),
    )


def create_app() -> "uvicorn.ASGIApplication":
    """Create FastAPI application with all dependencies."""
    redis_client = get_redis_client()
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    engine = WorkflowEngine(state_store, task_queue, redis_client)
    blueprint_parser = BlueprintParser()
    dockerinfo_parser = DockerInfoParser()
    breaker = get_circuit_breaker(redis_client)
    registry = ServiceRegistry(redis_client, breaker, engine)

    api = OrchestratorAPI(
        engine,
        blueprint_parser,
        dockerinfo_parser,
        redis_client,
        breaker=breaker,
        registry=registry,
    )
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
    logger.info(
        "Circuit breaker: threshold=%s reset_timeout=%ss",
        os.environ.get("CIRCUIT_FAILURE_THRESHOLD", "3"),
        os.environ.get("CIRCUIT_RESET_TIMEOUT", "60"),
    )
    logger.info(
        "Data policy: allow_inline=%s max_inline_bytes=%s; service scheme=%s; "
        "services key encryption=%s",
        os.environ.get("ALLOW_INLINE", "true"),
        os.environ.get("MAX_INLINE_BYTES", "65536"),
        os.environ.get("SERVICE_SCHEME", "http"),
        "on" if os.environ.get("ORCHESTRATOR_SECRET") else "off",
    )

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def get_app() -> "uvicorn.ASGIApplication":
    """Get or create the FastAPI application (for uvicorn import)."""
    return create_app()


if __name__ == "__main__":
    sys.exit(main())
