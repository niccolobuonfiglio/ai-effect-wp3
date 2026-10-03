"""Worker daemon that continuously polls for tasks."""

import json
import logging
import os
import signal
import sys
import time

import redis

from services.circuit_breaker import CircuitBreaker
from services.control_client import ControlClient, parse_tls_verify
from services.dockerinfo_parser import ServiceEndpoint
from services.log_service import configure_logging
from services.privacy import DataPolicy
from services.secrets import SecretBox, SecretDecryptionError, services_key_name
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.verifier import WorkflowVerifier
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

        self.state_store = RedisStateStore(redis_client)
        task_queue = RedisTaskQueue(redis_client)
        self.lease_seconds = float(os.environ.get("LEASE_SECONDS", "300"))
        self.engine = WorkflowEngine(
            self.state_store, task_queue, redis_client, lease_seconds=self.lease_seconds
        )

        self.verify = self._tls_verify(os.environ.get("SERVICE_TLS_VERIFY", "true"))
        self.control_client = ControlClient(verify=self.verify)

        self.breaker = CircuitBreaker(
            redis_client,
            failure_threshold=int(os.environ.get("CIRCUIT_FAILURE_THRESHOLD", "3")),
            reset_timeout=float(os.environ.get("CIRCUIT_RESET_TIMEOUT", "60")),
            degraded_threshold=int(os.environ.get("CIRCUIT_DEGRADED_THRESHOLD", "1")),
            initial_reset_timeout=float(os.environ.get("CIRCUIT_RESET_INITIAL", "5")),
            alert_repeat_seconds=float(os.environ.get("ALERT_REPEAT_SECONDS", "900")),
        )
        self.blocked_retry_delay = float(
            os.environ.get("BLOCKED_TASK_RETRY_DELAY", "30")
        )
        self.blocked_retry_initial = float(
            os.environ.get("BLOCKED_TASK_RETRY_INITIAL", "2")
        )
        # 0 (the default) keeps a blocked task waiting for as long as it takes.
        self.blocked_max_age = float(os.environ.get("BLOCKED_TASK_MAX_AGE", "0"))
        self.task_max_runtime = float(os.environ.get("TASK_MAX_RUNTIME", "3600"))
        self.policy = DataPolicy.from_env()
        self.secrets = SecretBox.from_env()
        # Fail running workflows that nothing can move on.
        self.stall_detection = os.environ.get(
            "STALL_DETECTION", "true"
        ).strip().lower() not in ("0", "false", "no", "off")
        # Check every workflow this worker completes.
        verify = os.environ.get("VERIFY_ON_COMPLETION", "true").strip().lower()
        self.verifier = (
            WorkflowVerifier(self.engine)
            if verify not in ("0", "false", "no", "off")
            else None
        )

    @staticmethod
    def _tls_verify(value: str) -> bool | str:
        """Read SERVICE_TLS_VERIFY as a flag, or as a path to a CA bundle."""
        return parse_tls_verify(value)

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
        return self.state_store.running_workflow_ids()

    def load_services_key(self, workflow_id: str) -> str | None:
        """Load and decrypt the services API key for a workflow."""
        stored = self.redis_client.get(services_key_name(workflow_id))
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
            blocked_max_age=self.blocked_max_age,
            task_max_runtime=self.task_max_runtime,
            policy=self.policy,
            verifier=self.verifier,
            stall_detection=self.stall_detection,
            stall_alert_repeat=float(os.environ.get("ALERT_REPEAT_SECONDS", "900")),
        )
        return worker.process_task(workflow_id, timeout=0)

    def run(self) -> None:
        """Main daemon loop."""
        logger.info("Worker daemon started, polling for tasks...")
        try:
            found = self.state_store.rebuild_running_index()
            logger.info(f"Running-workflow index rebuilt: {found} running")
        except Exception as e:
            logger.error(f"Could not rebuild running-workflow index: {e}")

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
    host = os.environ.get("HOSTNAME", "worker")
    configure_logging(
        log_dir="logs",
        log_file=f"worker-{host}.log",
        alerts_file=f"alerts-worker-{host}.jsonl",
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