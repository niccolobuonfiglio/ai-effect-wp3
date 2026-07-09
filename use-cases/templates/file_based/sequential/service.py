"""Service implementation - add your methods here.

Each method should be named execute_<MethodName> where MethodName
matches the operation name in the blueprint.
"""

from handler import DataReference, ExecuteRequest, ExecuteResponse, run


def execute_ProcessData(request: ExecuteRequest) -> ExecuteResponse:
    """Example: Process input data and return result.

    Replace this with your actual processing logic.
    """
    # Access inputs
    for input_ref in request.inputs:
        protocol = input_ref["protocol"]
        uri = input_ref["uri"]
        # Fetch and process data based on protocol...

    # Access parameters
    # threshold = request.parameters.get("threshold", 0.5)

    # Return output reference
    output_uri = f"s3://bucket/output/{request.task_id}.json"

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="s3",
            uri=output_uri,
            format="json",
        ),
    )


def execute_AnalyzeData(request: ExecuteRequest) -> ExecuteResponse:
    """Example: Another method.

    Add as many execute_* methods as needed for your service.
    """
    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="s3",
            uri=f"s3://bucket/analysis/{request.task_id}.json",
            format="json",
        ),
    )


if __name__ == "__main__":
    import sys
    run(sys.modules[__name__])
