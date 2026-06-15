import json
import os

from bytewax import operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow


def parse_json(line: str) -> dict | None:
    """Parse a line as JSON. Return None if invalid or missing required fields."""
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(record, dict):
        return None
    if "type" not in record or "device_id" not in record or "payload" not in record:
        return None
    if not isinstance(record["device_id"], str):
        return None
    if not isinstance(record["payload"], dict):
        return None

    return record


def stateful_handler(
    state: float | None,
    event: dict,
) -> tuple[float | None, str | None]:
    """Maintain threshold state per device and emit alerts for high temperature.

    Args:
        state: Current threshold for the device, or None if not yet set.
        event: The parsed JSON record.

    Returns:
        A tuple of (new_state, alert_json_or_None).
    """
    event_type = event["type"]
    payload = event["payload"]
    device_id = event["device_id"]

    # Initialize state to default threshold if not set
    if state is None:
        state = 100.0

    if event_type == "config":
        if "threshold" in payload and isinstance(payload["threshold"], (int, float)):
            state = float(payload["threshold"])
        return (state, None)

    elif event_type == "metric":
        if "temperature" in payload and isinstance(payload["temperature"], (int, float)):
            temperature = float(payload["temperature"])
            if temperature > state:
                alert = json.dumps({
                    "device_id": device_id,
                    "alert_type": "temperature_high",
                    "value": temperature,
                    "threshold": state,
                })
                return (state, alert)
        return (state, None)

    # Unknown event type — keep state, emit nothing
    return (state, None)


flow = Dataflow("iot_alerting")

# Read input lines
lines = op.input("read_input", flow, FileSource("input.jsonl"))

# Parse JSON and drop invalid records, keying by device_id
parsed = op.filter_map("parse_json", lines, parse_json)
keyed = op.key_on("key_by_device", parsed, lambda r: r["device_id"])

# Stateful processing: maintain threshold, emit alerts
alerts = op.stateful_map("process_events", keyed, stateful_handler)

# Filter out None values (non-alert events), keeping (key, value) tuples
alerts_only = op.filter_map("filter_alerts", alerts, lambda x: x if x[1] is not None else None)

# Write output
run_id = os.environ["ZEALT_RUN_ID"]
op.output("write_output", alerts_only, FileSink(f"output-{run_id}.jsonl"))
