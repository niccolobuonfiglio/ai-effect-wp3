"""Service implementation - add your methods here.

Each method should be named execute_<MethodName> where MethodName
matches the operation name in the blueprint.

For long-running operations, use task_manager to track progress.
"""

import time

from handler import (
    DataReference,
    ExecuteRequest,
    ExecuteResponse,
    TaskManager,
    run,
    run_in_background,
    task_manager,
)


def execute_QuickProcess(request: ExecuteRequest) -> ExecuteResponse:
    """Example: Quick operation that completes immediately.

    For fast operations, return complete status directly.
    """
    output_uri = f"s3://bucket/output/{request.task_id}.json"

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="s3",
            uri=output_uri,
            format="json",
        ),
    )


def execute_LongProcess(request: ExecuteRequest) -> ExecuteResponse:
    """Example: Long-running operation with progress tracking.

    For slow operations, register task and process in background.
    Uses orchestrator's task_id for tracking.
    """
    task_manager.register_task(request.task_id, request)
    run_in_background(request.task_id, _process_long_running, request)

    return ExecuteResponse(
        status="running",
        task_id=request.task_id,
    )


def _process_long_running(
    task_id: str,
    request: ExecuteRequest,
    manager: TaskManager,
) -> None:
    """Background worker for long-running task.

    Args:
        task_id: Task ID for progress updates.
        request: Original request with inputs/parameters.
        manager: TaskManager for updating progress.
    """
    try:
        # Simulate work with progress updates
        for progress in range(0, 101, 20):
            time.sleep(1)  # Replace with actual work
            manager.update_progress(task_id, progress)

        # Complete with output
        manager.complete_task(
            task_id,
            {
                "protocol": "s3",
                "uri": f"s3://bucket/output/{task_id}.json",
                "format": "json",
            },
        )

    except Exception as e:
        manager.fail_task(task_id, str(e))


if __name__ == "__main__":
    import sys
    run(sys.modules[__name__])
