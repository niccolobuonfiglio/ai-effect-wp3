"""Worker CLI for processing workflow tasks."""

import argparse
import json
import logging
import os
import sys

import redis

from services.control_client import ControlClient
from services.dockerinfo_parser import ServiceEndpoint
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.worker import Worker
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)


def get_redis_client() -> redis.Redis:
    """Create Redis client from environment."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def load_endpoints(redis_client: redis.Redis, workflow_id: str) -> dict[str, ServiceEndpoint]:
    """Load service endpoints from Redis."""
    endpoints_key = f"endpoints:{workflow_id}"
    endpoints_data = redis_client.hgetall(endpoints_key)

    if not endpoints_data:
        raise ValueError(f"No endpoints found for workflow {workflow_id}")

    endpoints = {}
    for name, data in endpoints_data.items():
        endpoint_dict = json.loads(data)
        endpoints[name] = ServiceEndpoint(**endpoint_dict)

    return endpoints


def main() -> int:
    """Run worker for a specific workflow."""
    parser = argparse.ArgumentParser(description="Orchestrator Worker")
    parser.add_argument(
        "workflow_id",
        help="Workflow ID to process tasks for",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="Timeout in seconds for waiting on tasks (0 = blocking)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Poll interval in seconds for async tasks (default: 5.0)",
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default=os.environ.get("LOG_LEVEL", "info").lower(),
        help="Log level (default: info)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info(f"Starting worker for workflow {args.workflow_id}")

    redis_client = get_redis_client()
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    engine = WorkflowEngine(state_store, task_queue, redis_client)

    # Load endpoints from Redis
    endpoints = load_endpoints(redis_client, args.workflow_id)
    logger.info(f"Loaded {len(endpoints)} service endpoints")

    # Create worker
    client = ControlClient()
    worker = Worker(engine, client, endpoints, poll_interval=args.poll_interval)

    # Run until workflow complete
    logger.info("Processing tasks...")
    worker.run(args.workflow_id, timeout=args.timeout)

    logger.info("Workflow complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
