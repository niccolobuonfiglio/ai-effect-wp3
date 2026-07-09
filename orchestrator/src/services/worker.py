"""Worker that processes tasks from queue via service control endpoints."""

import time

from models.data_reference import DataReference
from services.control_client import ControlClient, ControlClientError
from services.dockerinfo_parser import ServiceEndpoint
from services.workflow_engine import WorkflowEngine


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
    ):
        """Initialize worker with dependencies."""
        if engine is None:
            raise ValueError("engine is required")
        if client is None:
            raise ValueError("client is required")
        if endpoints is None:
            raise ValueError("endpoints is required")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        self._engine = engine
        self._client = client
        self._endpoints = endpoints
        self._poll_interval = poll_interval

    def process_task(self, workflow_id: str, timeout: float = 0) -> bool:
        """Process single task from queue.

        Returns True if a task was processed, False if queue was empty.
        """
        if not workflow_id or not workflow_id.strip():
            raise ValueError("workflow_id is required")

        # Claim task from queue
        task = self._engine.claim_task(workflow_id, timeout)
        if task is None:
            return False

        try:
            # Parse node_key to get container and method
            container_name, method = self._parse_node_key(task.node_key)

            # Look up endpoint
            endpoint = self._get_endpoint(container_name)
            base_url = f"http://{endpoint.address}:{endpoint.port}"

            # Convert input_refs to list
            inputs = list(task.input_refs) if task.input_refs else None

            # Execute service call
            response = self._client.execute(
                base_url=base_url,
                method=method,
                workflow_id=workflow_id,
                task_id=task.task_id,
                inputs=inputs,
            )

            # Handle response
            if response.status == "failed":
                error = response.error or "Service returned failed status"
                self._engine.fail_task(workflow_id, task.task_id, error)
                return True

            if response.status == "complete":
                output_refs = [response.output] if response.output else []
            elif response.status == "running":
                output_refs = self._poll_until_complete(
                    base_url, response.task_id, workflow_id, task.task_id
                )
            else:
                self._engine.fail_task(
                    workflow_id, task.task_id, f"Unknown status: {response.status}"
                )
                return True

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
