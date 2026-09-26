"""Data Analyzer service - analyzes energy data for anomalies and efficiency."""

import logging
import sys
from pathlib import Path

import pandas as pd

from common.integrity import IntegrityError, describe_file, verify_file
from common.sequential import DataReference, ExecuteRequest, ExecuteResponse, run

logger = logging.getLogger(__name__)


def _get_input_reference(inputs: list[dict]) -> dict:
    """The CSV file reference among the inputs, or an empty dict."""
    for inp in inputs:
        if inp.get("protocol") == "file" and inp.get("format") == "csv":
            return inp
    return {}


def execute_AnalyzeData(request: ExecuteRequest) -> ExecuteResponse:
    """Analyze energy data for anomalies and efficiency.

    Input: File path to raw energy CSV.
    Parameters: anomaly_threshold (default 0.5)
    Output: File path to analyzed CSV.
    """
    # Get input file path
    input_ref = _get_input_reference(request.inputs)
    input_path = input_ref.get("uri", "")
    if not input_path:
        return ExecuteResponse(status="failed", error="No input file provided")

    # Get threshold from parameters
    anomaly_threshold = request.parameters.get("anomaly_threshold", 0.5)

    logger.info(f"AnalyzeData: input={input_path}, threshold={anomaly_threshold}")

    # Read input CSV
    input_file = Path(input_path)
    if not input_file.exists():
        return ExecuteResponse(status="failed", error=f"Input file not found: {input_path}")

    # Refuse a file that differs from what the previous service wrote
    try:
        verify_file(input_ref, input_file)
    except IntegrityError as e:
        return ExecuteResponse(status="failed", error=f"Input rejected: {e}")

    df = pd.read_csv(input_file)

    # Perform analysis
    analyzed_data = []
    anomaly_count = 0
    total_efficiency = 0

    for _, row in df.iterrows():
        power = float(row["power_consumption"])
        voltage = float(row["voltage"])
        current = float(row["current"])

        # Calculate efficiency
        efficiency = power / (voltage * current) if (voltage * current) != 0 else 0
        total_efficiency += efficiency

        # Detect anomalies
        anomaly = efficiency < anomaly_threshold or efficiency > 0.95
        if anomaly:
            anomaly_count += 1

        status = "anomaly" if anomaly else "normal"

        analyzed_data.append({
            "timestamp": row["timestamp"],
            "household_id": row["household_id"],
            "power": power,
            "efficiency": efficiency,
            "status": status,
            "anomaly_detected": anomaly,
        })

    # Save analyzed data
    output_path = Path("data/analyzed_energy.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    analyzed_df = pd.DataFrame(analyzed_data)
    analyzed_df.to_csv(output_path, index=False)

    avg_efficiency = total_efficiency / len(analyzed_data) if analyzed_data else 0
    logger.info(
        f"Analyzed {len(analyzed_data)} records, {anomaly_count} anomalies, "
        f"avg efficiency={avg_efficiency:.3f}"
    )

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="file",
            uri=str(output_path.absolute()),
            format="csv",
            # Lets the next service check the file arrived intact
            **describe_file(output_path),
        ),
    )


if __name__ == "__main__":
    run(sys.modules[__name__])
