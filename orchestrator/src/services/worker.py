"""Worker that processes tasks from queue via service control endpoints."""

import logging
import time
from datetime import datetime, timezone

from models.data_reference import DataReference
from models.state import WorkflowStatus
from services.backoff import backoff_delay
from services.circuit_breaker import ALERT_SOURCE, CircuitBreaker
from services.control_client import ControlClient, ControlClientError
from services.dockerinfo_parser import ServiceEndpoint
from services.integrity import IntegrityError, verify_all
from services.log_service import get_alerts_logger
from services.privacy import DataPolicy, DataPolicyError
from services.verifier import WorkflowVerifier
from services.workflow_engine import WorkflowEngine

logger = logging.getLogger(__name__)
alerts_logger = get_alerts_logger()


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
        blocked_retry_initial: float | None = None,
        blocked_max_age: float | None = None,
        task_max_runtime: float | None = None,
        verifier: WorkflowVerifier | None = None,
        stall_detection: bool = False,
        stall_alert_repeat: float = 900.0,
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
            blocked_retry_delay: Longest wait before a BLOCKED task is
                re-queued.
            blocked_retry_initial: Wait before the first retry; it doubles on
                each further deferral, up to ``blocked_retry_delay``. None
                means no backoff: every wait is ``blocked_retry_delay``.
            blocked_max_age: Fail a task that has been blocked for longer than
                this many seconds, instead of retrying it forever. None or 0
                means no limit.
            task_max_runtime: Fail a task whose service keeps reporting
                "running" for longer than this many seconds. None or 0 means
                no limit. The service answered, so this does not count against
                the circuit breaker.
            policy: Limits on the data references a service may return.
            verifier: When set, a workflow this worker completes is checked
                against the engine's properties, and a failed check raises a
                WORKFLOW_VERIFICATION_FAILED alert (docs/VERIFICATION.md).
            stall_detection: When a poll finds nothing to claim, check whether
                the workflow is stalled - running with nothing queued, parked
                or leased - and fail it with a WORKFLOW_STALLED alert
                (docs/STALLS.md).
            stall_alert_repeat: Seconds before the alert for a stall that
                cannot be resolved is repeated.
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
        if blocked_retry_initial is not None and blocked_retry_initial <= 0:
            raise ValueError("blocked_retry_initial must be positive")
        if blocked_max_age is not None and blocked_max_age < 0:
            raise ValueError("blocked_max_age must be non-negative")
        if task_max_runtime is not None and task_max_runtime < 0:
            raise ValueError("task_max_runtime must be non-negative")

        self._engine = engine
        self._client = client
        self._endpoints = endpoints
        self._poll_interval = poll_interval
        self._breaker = breaker
        self._blocked_retry_delay = blocked_retry_delay
        self._blocked_retry_initial = blocked_retry_initial
        self._blocked_max_age = blocked_max_age
        self._task_max_runtime = task_max_runtime
        self._policy = policy or DataPolicy.from_env()
        self._verifier = verifier
        self._stall_detection = stall_detection
        self._stall_alert_repeat = stall_alert_repeat

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
            self._check_stalled(workflow_id)
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
                        container_name,
                        f"Circuit open for service '{container_name}' at "
                        f"{base_url} - service marked UNHEALTHY, call not "
                        f"attempted",
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

            # An inline payload that does not match the checksum or size its
            # own reference states was corrupted or mislabelled on the way;
            # passing it on would hand bad data to the next task.
            try:
                verify_all(output_refs)
            except IntegrityError as e:
                self._engine.fail_task(
                    workflow_id,
                    task.task_id,
                    f"Service '{container_name}' returned data that fails its "
                    f"integrity check: {e}",
                )
                return True

            # Successful round trip - close the circuit if it was open
            self._record_success(container_name, base_url)

            # Complete task with outputs
            self._engine.complete_task(workflow_id, task.task_id, output_refs)
            self._verify_if_completed(workflow_id)
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

    def _check_stalled(self, workflow_id: str) -> None:
        """Fail a workflow that nothing can move on any more, and say so.

        Runs only when this poll found nothing to claim, so a workflow with
        work in it costs one ``EXISTS``. A stall held by failed tasks fails
        the workflow (``resume`` then applies). One without failed tasks
        means the state is damaged; it is only reported, at most once per
        ``stall_alert_repeat`` seconds across all workers.
        """
        if not self._stall_detection:
            return
        try:
            failed = self._engine.fail_if_stalled(workflow_id)
        except Exception as e:
            logger.warning("Could not check workflow %s for a stall: %s", workflow_id, e)
            return
        if failed is None:
            return
        if failed:
            summary = (
                f"nothing left to run while {len(failed)} task(s) failed and were "
                f"not retried; workflow marked failed, resume it to continue"
            )
        else:
            # Reported once per period by whichever worker gets there first.
            key = f"stall_alerted:{workflow_id}"
            if not self._engine.redis.set(key, "1", nx=True, ex=int(self._stall_alert_repeat) or None):
                return
            summary = (
                "nothing queued, parked or leased and no task failed: the state "
                "is inconsistent, see GET /workflows/{id}/verify"
            )
        alerts_logger.error(
            "WORKFLOW STALLED: %s (%s)",
            workflow_id,
            summary,
            extra={
                "event": "workflow_stalled",
                "alert": "WORKFLOW_STALLED",
                "severity": "error",
                "source": ALERT_SOURCE,
                "workflow_id": workflow_id,
                "failed_tasks": failed,
                "marked_failed": bool(failed),
                "summary": summary,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _verify_if_completed(self, workflow_id: str) -> None:
        """Check a workflow this worker just completed; alert if it fails.

        The workflow is already completed when this runs, so a client sees
        the result without waiting for it. A problem here never fails the
        task or stops the worker: the work is done, and the alert says the
        state that records it is not what it should be.
        """
        if self._verifier is None:
            return
        try:
            if self._engine.get_workflow_status(workflow_id).status != WorkflowStatus.COMPLETED:
                return
            failed = [r for r in self._verifier.verify(workflow_id) if not r.passed]
        except Exception as e:
            logger.warning("Could not verify workflow %s: %s", workflow_id, e)
            return
        if not failed:
            logger.info(
                "Workflow %s completed and verified", workflow_id,
                extra={"event": "workflow_verified", "workflow_id": workflow_id},
            )
            return
        problems = [f"{r.name}: {p}" for r in failed for p in r.problems]
        alerts_logger.error(
            "WORKFLOW VERIFICATION FAILED: %s (%d problem(s): %s)",
            workflow_id,
            len(problems),
            "; ".join(problems[:3]),
            extra={
                "event": "workflow_verification_failed",
                "alert": "WORKFLOW_VERIFICATION_FAILED",
                "severity": "error",
                "source": ALERT_SOURCE,
                "workflow_id": workflow_id,
                "failed_checks": [r.name for r in failed],
                "summary": "; ".join(problems[:3]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

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
            container_name,
            f"Service '{container_name}' unavailable: {error}",
        )
        return True

    def _retry_delay(self, attempt: int) -> float:
        """Wait before retry ``attempt`` of a blocked task."""
        if self._blocked_retry_delay == 0:
            return 0.0
        initial = self._blocked_retry_initial or self._blocked_retry_delay
        return backoff_delay(attempt, initial, self._blocked_retry_delay)

    def _defer(
        self, workflow_id: str, task_id: str, container_name: str, cause: str
    ) -> None:
        """Park a task until its service is healthy, keeping the workflow alive.

        ``cause`` says why the task cannot run now; the advice to the operator
        is added here, so a task that gives up does not claim it will be
        retried. A task blocked for longer than ``blocked_max_age`` fails
        instead, so a workflow waiting on a service that never returns does not
        stay ``running`` forever.
        """
        attempt = int(self._engine.record_blocked_attempt(workflow_id, task_id))
        age = float(self._engine.blocked_age(workflow_id, task_id))
        if self._blocked_max_age and age > self._blocked_max_age:
            self._engine.fail_task(
                workflow_id,
                task_id,
                f"{cause}. Giving up after {int(age)}s blocked (limit "
                f"{int(self._blocked_max_age)}s) and {attempt} attempts; once "
                f"the service is back, continue with POST /workflows/"
                f"{workflow_id}/resume",
            )
            logger.error(
                "Task %s of workflow %s failed after %ds blocked: %s",
                task_id,
                workflow_id,
                int(age),
                cause,
                extra={
                    "event": "blocked_task_expired",
                    "workflow_id": workflow_id,
                    "task_id": task_id,
                    "blocked_seconds": int(age),
                    "attempts": attempt,
                },
            )
            return

        delay = self._retry_delay(attempt)
        reason = (
            f"{cause}. Task will be retried; recover via POST "
            f"/orchestrator/health/services/{container_name}/recover"
        )
        self._engine.defer_task(workflow_id, task_id, reason, delay=delay)
        logger.warning(
            "Task %s of workflow %s blocked (attempt %d), retry in %ss: %s",
            task_id,
            workflow_id,
            attempt,
            delay,
            reason,
            extra={
                "event": "task_blocked",
                "workflow_id": workflow_id,
                "task_id": task_id,
                "attempt": attempt,
                "retry_delay": delay,
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
        """Poll service status until complete, or until the task times out.

        Each poll also renews the task's lease, so a task that legitimately
        runs for a long time is not reclaimed by another worker, while a task
        whose service never finishes is failed after ``task_max_runtime``.
        """
        started = time.time()
        while True:
            status = self._client.get_status(base_url, service_task_id)

            if status.status == "complete":
                output = self._client.get_output(base_url, service_task_id)
                return [output.output] if output.output else []

            if status.status == "failed":
                error = status.error or "Service task failed"
                raise WorkerError(error)

            elapsed = time.time() - started
            if self._task_max_runtime and elapsed >= self._task_max_runtime:
                raise WorkerError(
                    f"Service did not finish within {int(self._task_max_runtime)}s "
                    f"(still reporting '{status.status}' after {int(elapsed)}s)"
                )

            # Still running - keep the lease, then wait and poll again
            self._engine.renew_lease(workflow_id, task_id)
            time.sleep(self._poll_interval)
