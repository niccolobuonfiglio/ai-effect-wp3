"""Worker daemon that continuously polls for tasks."""

import json
import logging
import os
import signal
import sys
import time

import redis

from services.circuit_breaker import CircuitBreaker
from services.control_client import ControlClient
from services.dockerinfo_parser import ServiceEndpoint
from services.log_service import configure_logging
from services.privacy import DataPolicy
from services.secrets import SecretBox, SecretDecryptionError
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.worker import Worker
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger("worker_daemon")


class WorkerDaemon:
    """Daemon that polls for running workflows and processes their tasks."""

    def __init__(
        self,
        redis_client: redis.Redis,
        poll_interval: float = 1.0,
    ):
        self.redis_client = redis_client
        self.poll_interval = poll_interval
        self.running = True

        state_store = RedisStateStore(redis_client)
        task_queue = RedisTaskQueue(redis_client)
        self.engine = WorkflowEngine(state_store, task_queue, redis_client)

        # TLS verification for https service endpoints; a CA bundle path is
        # also accepted for private certificates.
        self.verify = self._tls_verify(os.environ.get("SERVICE_TLS_VERIFY", "true"))
        self.control_client = ControlClient(verify=self.verify)

        # Shared circuit breaker: state lives in Redis, so every worker
        # replica and the API observe the same service health.
        self.breaker = CircuitBreaker(
            redis_client,
            failure_threshold=int(os.environ.get("CIRCUIT_FAILURE_THRESHOLD", "3")),
            reset_timeout=float(os.environ.get("CIRCUIT_RESET_TIMEOUT", "60")),
            degraded_threshold=int(os.environ.get("CIRCUIT_DEGRADED_THRESHOLD", "1")),
            initial_reset_timeout=float(os.environ.get("CIRCUIT_RESET_INITIAL", "5")),
        )
        self.blocked_retry_delay = float(
            os.environ.get("BLOCKED_TASK_RETRY_DELAY", "30")
        )
        self.blocked_retry_initial = float(
            os.environ.get("BLOCKED_TASK_RETRY_INITIAL", "2")
        )
        self.policy = DataPolicy.from_env()
        self.secrets = SecretBox.from_env()

    @staticmethod
    def _tls_verify(value: str) -> bool | str:
        """Read SERVICE_TLS_VERIFY as a flag, or as a path to a CA bundle."""
        setting = (value or "").strip()
        if setting.lower() in ("0", "false", "no", "off"):
            return False
        if setting.lower() in ("", "1", "true", "yes", "on"):
            return True
        return setting

    def load_endpoints(self, workflow_id: str) -> dict[str, ServiceEndpoint]:
        """Load service endpoints for a workflow from Redis."""
        endpoints_key = f"endpoints:{workflow_id}"
        endpoints_data = self.redis_client.hgetall(endpoints_key)
        if not endpoints_data:
            return {}

        endpoints = {}
        for name, data in endpoints_data.items():
            ep_dict = json.loads(data)
            endpoints[name] = ServiceEndpoint(**ep_dict)
        return endpoints

    def get_running_workflows(self) -> list[str]:
        """Get all workflows with running status."""
        running = []
        cursor = 0
        while True:
            cursor, keys = self.redis_client.scan(
                cursor, match="workflow:*", count=100
            )
            for key in keys:
                # Skip task keys and other workflow sub-keys
                if key.count(":") > 1:
                    continue
                try:
                    data = self.redis_client.get(key)
                    if data:
                        workflow = json.loads(data)
                        if workflow.get("status") == "running":
                            workflow_id = key.split(":")[1]
                            running.append(workflow_id)
                except (json.JSONDecodeError, IndexError):
                    continue
            if cursor == 0:
                break
        return running

    def load_services_key(self, workflow_id: str) -> str | None:
        """Load and decrypt the services API key for a workflow."""
        stored = self.redis_client.get(f"services_key:{workflow_id}")
        try:
            return self.secrets.decrypt(stored)
        except SecretDecryptionError as e:
            logger.error(
                "Cannot read services API key for workflow %s: %s", workflow_id, e,
                extra={"event": "services_key_unreadable", "workflow_id": workflow_id},
            )
            return None

    def process_workflow(self, workflow_id: str) -> bool:
        """Process one task for a workflow.

        Returns True if a task was processed, False otherwise.
        """
        endpoints = self.load_endpoints(workflow_id)
        if not endpoints:
            logger.warning(f"No endpoints for workflow {workflow_id}, skipping")
            return False

        services_key = self.load_services_key(workflow_id)
        client = (
            ControlClient(api_key=services_key, verify=self.verify)
            if services_key
            else self.control_client
        )
        worker = Worker(
            self.engine,
            client,
            endpoints,
            breaker=self.breaker,
            blocked_retry_delay=self.blocked_retry_delay,
            blocked_retry_initial=self.blocked_retry_initial,
            policy=self.policy,
        )
        return worker.process_task(workflow_id, timeout=0)

    def run(self) -> None:
        """Main daemon loop."""
        logger.info("Worker daemon started, polling for tasks...")

        while self.running:
            try:
                workflows = self.get_running_workflows()
                processed_any = False

                for workflow_id in workflows:
                    if not self.running:
                        break
                    try:
                        if self.process_workflow(workflow_id):
                            processed_any = True
                            logger.info(f"Processed task for workflow {workflow_id}")
                    except Exception as e:
                        logger.error(f"Error processing workflow {workflow_id}: {e}")

                # Only sleep if no tasks were processed
                if not processed_any:
                    time.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Error in daemon loop: {e}")
                time.sleep(self.poll_interval)

        logger.info("Worker daemon stopped")

    def stop(self) -> None:
        """Signal daemon to stop."""
        self.running = False


def main() -> int:
    # Configure logging with file rotation
    configure_logging(
        log_dir="logs",
        log_file="worker_daemon.log",
        level=logging.INFO,
    )

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    poll_interval = float(os.environ.get("WORKER_POLL_INTERVAL", "1.0"))

    logger.info(f"Connecting to Redis at {redis_url}")
    redis_client = redis.Redis.from_url(redis_url, decode_responses=True)

    # Test connection
    try:
        redis_client.ping()
        logger.info("Redis connection established")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return 1

    daemon = WorkerDaemon(redis_client, poll_interval)

    def handle_signal(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        daemon.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    daemon.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())