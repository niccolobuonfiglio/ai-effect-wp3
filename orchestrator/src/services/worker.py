"""Worker that processes tasks from queue via service control endpoints."""

import logging
import time

from models.data_reference import DataReference
from services.circuit_breaker import CircuitBreaker
from services.control_client import ControlClient, ControlClientError
from services.dockerinfo_parser import ServiceEndpoint
from services.privacy import DataPolicy, DataPolicyError
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)


class WorkerError(Exception):
    """Raised when worker encounters an error."""

    pass


class Worker:
    """Processes tasks from queue by calling services."""

    def __init__(
        self,
        engine: WorkflowEngine,
        client: ControlClient,
        endpoints: dict[str, ServiceEndpoint],
        poll_interval: float = 5.0,
        breaker: CircuitBreaker | None = None,
        blocked_retry_delay: float = 30.0,
        policy: DataPolicy | None = None,
    ):
        """Initialize worker with dependencies.

        Args:
            engine: Workflow engine for claiming and completing tasks.
            client: HTTP client for service control endpoints.
            endpoints: Mapping of container name to service endpoint.
            poll_interval: Seconds between status polls for running tasks.
            breaker: Optional circuit breaker tracking per-service health.
                When set, tasks for unavailable services are parked (BLOCKED)
                instead of failing the workflow.
            blocked_retry_delay: Seconds before a BLOCKED task is re-queued.
            policy: Limits on the data references a service may return.
        """
        if engine is None:
            raise ValueError("engine is required")
        if client is None:
            raise ValueError("client is required")
        if endpoints is None:
            raise ValueError("endpoints is required")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if blocked_retry_delay < 0:
            raise ValueError("blocked_retry_delay must be non-negative")

        self._engine = engine
        self._client = client
        self._endpoints = endpoints
        self._poll_interval = poll_interval
        self._breaker = breaker
        self._blocked_retry_delay = blocked_retry_delay
        self._policy = policy or DataPolicy.from_env()

    def process_task(self, workflow_id: str, timeout: float = 0) -> bool:
        """Process single task from queue.

        Returns True if a task was processed, False if queue was empty.
        """
        if not workflow_id or not workflow_id.strip():
            raise ValueError("workflow_id is required")

        # Tasks parked on an unhealthy service become claimable again once
        # their retry delay has elapsed.
        self._engine.release_due_tasks(workflow_id)

        # Claim task from queue
        task = self._engine.claim_task(workflow_id, timeout)
        if task is None:
            return False

        try:
            # Parse node_key to get container and method
            container_name, method = self._parse_node_key(task.node_key)

            # Look up endpoint
            endpoint = self._get_endpoint(container_name)
            base_url = endpoint.base_url

            # Circuit breaker gate: skip the call entirely if the service is
            # known to be down, until an operator recovers it or the reset
            # timeout allows a probe.
            if self._breaker is not None:
                self._breaker.register(container_name, base_url)
                if not self._breaker.allow(container_name):
                    self._defer(
                        workflow_id,
                        task.task_id,
                        f"Circuit open for service '{container_name}' at "
                        f"{base_url} - service marked UNHEALTHY, call not "
                        f"attempted. Recover via POST /orchestrator/health/"
                        f"services/{container_name}/recover",
                    )
                    return True

            # Convert input_refs to list
            inputs = list(task.input_refs) if task.input_refs else None

            # Execute service call
            try:
                response = self._client.execute(
                    base_url=base_url,
                    method=method,
                    workflow_id=workflow_id,
                    task_id=task.task_id,
                    inputs=inputs,
                )
            except ControlClientError as e:
                if self._handle_unavailable(
                    workflow_id, task.task_id, container_name, base_url, e
                ):
                    return True
                raise

            # Handle response. A failure reported by the service proves it is
            # reachable, so it fails the task but does not count against the
            # breaker (e.g. a crashed downstream dependency of the service).
            if response.status == "failed":
                error = response.error or "Service returned failed status"
                self._engine.fail_task(workflow_id, task.task_id, error)
                return True

            if response.status == "complete":
                output_refs = [response.output] if response.output else []
            elif response.status == "running":
                try:
                    output_refs = self._poll_until_complete(
                        base_url, response.task_id, workflow_id, task.task_id
                    )
                except ControlClientError as e:
                    # The service went away mid-task; its task state is likely
                    # lost, so the task is re-executed once it is back.
                    if self._handle_unavailable(
                        workflow_id, task.task_id, container_name, base_url, e
                    ):
                        return True
                    raise
            else:
                self._engine.fail_task(
                    workflow_id, task.task_id, f"Unknown status: {response.status}"
                )
                return True

            # A service that returns a huge inline payload would put that data
            # into Redis and the API; reject it instead.
            try:
                self._policy.check_all(output_refs)
            except DataPolicyError as e:
                self._engine.fail_task(
                    workflow_id,
                    task.task_id,
                    f"Service '{container_name}' returned data that breaks the "
                    f"orchestrator data policy: {e}",
                )
                return True

            # Successful round trip - close the circuit if it was open
            self._record_success(container_name, base_url)

            # Complete task with outputs
            self._engine.complete_task(workflow_id, task.task_id, output_refs)
            return True

        except ControlClientError as e:
            self._engine.fail_task(workflow_id, task.task_id, str(e))
            return True
        except WorkerError as e:
            self._engine.fail_task(workflow_id, task.task_id, str(e))
            return True

    def run(self, workflow_id: str, timeout: float = 0) -> None:
        """Run worker loop until workflow complete or queue empty."""
        if not workflow_id or not workflow_id.strip():
            raise ValueError("workflow_id is required")

        while not self._engine.is_workflow_complete(workflow_id):
            processed = self.process_task(workflow_id, timeout)
            if not processed:
                # Queue empty but workflow not complete - wait a bit
                if timeout > 0:
                    continue
                break

    def _handle_unavailable(
        self,
        workflow_id: str,
        task_id: str,
        container_name: str,
        base_url: str,
        error: ControlClientError,
    ) -> bool:
        """Handle a client error; return True if the task was deferred.

        Only transient errors (unreachable, timeout, 5xx) say something about
        service availability: they count against the breaker and, when a
        breaker is configured, park the task. Other errors are left to fail it.
        """
        if not error.transient or self._breaker is None:
            return False

        self._breaker.record_failure(container_name, base_url, str(error))
        self._defer(
            workflow_id,
            task_id,
            f"Service '{container_name}' unavailable: {error}. Task will be "
            f"retried; recover via POST /orchestrator/health/services/"
            f"{container_name}/recover",
        )
        return True

    def _defer(self, workflow_id: str, task_id: str, reason: str) -> None:
        """Park a task until its service is healthy, keeping the workflow alive."""
        self._engine.defer_task(
            workflow_id, task_id, reason, delay=self._blocked_retry_delay
        )
        logger.warning(
            "Task %s of workflow %s blocked, retry in %ss: %s",
            task_id,
            workflow_id,
            self._blocked_retry_delay,
            reason,
            extra={
                "event": "task_blocked",
                "workflow_id": workflow_id,
                "task_id": task_id,
                "retry_delay": self._blocked_retry_delay,
            },
        )

    def _record_success(self, container_name: str, base_url: str) -> None:
        """Record a service success with the breaker, if one is configured."""
        if self._breaker is not None:
            self._breaker.record_success(container_name, base_url)

    def _parse_node_key(self, node_key: str) -> tuple[str, str]:
        """Parse node_key into container_name and method."""
        if ":" not in node_key:
            raise WorkerError(f"Invalid node_key format: {node_key}")
        parts = node_key.split(":", 1)
        if not parts[0] or not parts[1]:
            raise WorkerError(f"Invalid node_key format: {node_key}")
        return parts[0], parts[1]

    def _get_endpoint(self, container_name: str) -> ServiceEndpoint:
        """Get endpoint for container."""
        if container_name not in self._endpoints:
            raise WorkerError(f"Endpoint not found for: {container_name}")
        return self._endpoints[container_name]

    def _poll_until_complete(
        self,
        base_url: str,
        service_task_id: str,
        workflow_id: str,
        task_id: str,
    ) -> list[DataReference]:
        """Poll service status until complete."""
        while True:
            status = self._client.get_status(base_url, service_task_id)

            if status.status == "complete":
                output = self._client.get_output(base_url, service_task_id)
                return [output.output] if output.output else []

            if status.status == "failed":
                error = status.error or "Service task failed"
                raise WorkerError(error)

            # Still running - wait and poll again
            time.sleep(self._poll_interval)
