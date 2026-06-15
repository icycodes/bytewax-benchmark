import json
import os

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow

run_id = os.environ["ZEALT_RUN_ID"]
DEFAULT_THRESHOLD = 100.0

flow = Dataflow("iot_pipeline")

# Read lines from input.jsonl
inp = op.input("read_input", flow, FileSource("input.jsonl"))


# Parse JSON lines; drop invalid JSON or lines missing required fields
def parse_json(line):
    try:
        data = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if "type" not in data or "device_id" not in data or "payload" not in data:
        return None
    return data


parsed = op.filter_map("parse_json", inp, parse_json)

# Key the stream by device_id for stateful processing
keyed = op.key_on("key_device", parsed, lambda x: x["device_id"])


# Maintain per-device threshold state and emit alerts
def process_event(state, event):
    threshold = state if state is not None else DEFAULT_THRESHOLD
    event_type = event["type"]
    payload = event["payload"]
    device_id = event["device_id"]

    if event_type == "config":
        new_threshold = payload.get("threshold", threshold)
        return (new_threshold, [])

    if event_type == "metric":
        temperature = payload.get("temperature")
        if temperature is not None and temperature > threshold:
            alert = json.dumps(
                {
                    "device_id": device_id,
                    "alert_type": "temperature_high",
                    "value": temperature,
                    "threshold": threshold,
                }
            )
            return (threshold, [alert])
        return (threshold, [])

    # Unknown event type – keep current state, emit nothing
    return (threshold, [])


processed = op.stateful_flat_map("process_event", keyed, process_event)

# Write alerts to the output file (keyed stream needed for sink routing)
op.output("write_output", processed, FileSink(f"output-{run_id}.jsonl"))