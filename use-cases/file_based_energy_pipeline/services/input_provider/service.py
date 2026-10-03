"""Input Provider service - provides initial configuration for the pipeline."""

import base64
import json
import logging
import sys

from common.sequential import DataReference, ExecuteRequest, ExecuteResponse, run

logger = logging.getLogger(__name__)

DEFAULT_NUM_RECORDS = 10
MAX_NUM_RECORDS = 1_000_000


def _requested_num_records(inputs: list[dict]) -> int:
    """``num_records`` from an inline JSON workflow input, else the default.

    The workflow's initial inputs reach this service as its own inputs. An
    inline JSON input may set ``num_records``, so that the evaluation can vary
    the size of the data the pipeline produces (PROTOCOL.md, Amendment 4).
    Every other field, and every input that is not inline JSON, is ignored.
    The input is not checksum-verified here: without it, or with an input that
    does not set ``num_records``, the output is exactly the one it always was.
    """
    for inp in inputs:
        if inp.get("protocol") != "inline" or inp.get("format") != "json":
            continue
        try:
            config = json.loads(base64.b64decode(inp.get("uri", "")).decode())
        except (ValueError, UnicodeDecodeError):
            continue
        value = config.get("num_records") if isinstance(config, dict) else None
        if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_NUM_RECORDS:
            return value
    return DEFAULT_NUM_RECORDS


def execute_GetConfiguration(request: ExecuteRequest) -> ExecuteResponse:
    """Return the configuration for the pipeline.

    Output is an inline JSON DataReference containing:
    - num_records: Number of records to generate (default 10, or the value of
      an inline JSON workflow input)
    - output_format: Output format (csv)
    """
    num_records = _requested_num_records(request.inputs)
    output_format = "csv"

    logger.info(f"GetConfiguration: num_records={num_records}, format={output_format}")

    # Encode configuration as inline JSON
    config = {
        "num_records": num_records,
        "output_format": output_format,
    }
    config_json = json.dumps(config)
    config_b64 = base64.b64encode(config_json.encode()).decode()

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="inline",
            uri=config_b64,
            format="json",
        ),
    )


if __name__ == "__main__":
    run(sys.modules[__name__])
