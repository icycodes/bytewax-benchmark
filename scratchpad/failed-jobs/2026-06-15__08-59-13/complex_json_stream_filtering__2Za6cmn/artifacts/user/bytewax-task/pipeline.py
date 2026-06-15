import json
import os
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# Define the dataflow
flow = Dataflow("iot_pipeline")

# Read run-id from environment variable
run_id = os.environ.get("ZEALT_RUN_ID", "default")
output_filename = f"output-{run_id}.jsonl"

# 1. Read lines from input.jsonl
stream = op.input("input_step", flow, FileSource("input.jsonl"))

# 2. Parse and validate JSON events, keying by device_id
def parse_and_validate(line: str) -> Optional[Tuple[str, dict]]:
    try:
        data = json.loads(line)
    except Exception:
        return None
    
    if not isinstance(data, dict):
        return None
        
    # Check required fields
    if "type" not in data or "device_id" not in data or "payload" not in data:
        return None
        
    device_id = data["device_id"]
    if not isinstance(device_id, str):
        return None
        
    if not isinstance(data["payload"], dict):
        return None
        
    if not isinstance(data["type"], str):
        return None
        
    return (device_id, data)

parsed_stream = op.filter_map("parse_and_validate_step", stream, parse_and_validate)

# 3. Maintain threshold state per device and process events
def process_event(state: Optional[float], event: dict) -> Tuple[Optional[float], Optional[dict]]:
    if state is None:
        state = 100.0
        
    event_type = event.get("type")
    payload = event.get("payload", {})
    device_id = event.get("device_id")
    
    if event_type == "config":
        if "threshold" in payload:
            threshold_val = payload["threshold"]
            if isinstance(threshold_val, (int, float)):
                state = float(threshold_val)
        return state, None
        
    elif event_type == "metric":
        if "temperature" in payload:
            temp_val = payload["temperature"]
            if isinstance(temp_val, (int, float)):
                if float(temp_val) > state:
                    alert = {
                        "device_id": device_id,
                        "alert_type": "temperature_high",
                        "value": temp_val,
                        "threshold": state
                    }
                    return state, alert
        return state, None
        
    return state, None

stateful_stream = op.stateful_map("stateful_processing_step", parsed_stream, process_event)

# 4. Filter out None alerts and convert to Tuple[str, str] for FileSink
def format_alert(item: Tuple[str, Optional[dict]]) -> Optional[Tuple[str, str]]:
    device_id, alert = item
    if alert is not None:
        return (device_id, json.dumps(alert))
    return None

alerts_stream = op.filter_map("format_alerts_step", stateful_stream, format_alert)

# 5. Write resulting alert JSON strings to output-${run-id}.jsonl
op.output("output_step", alerts_stream, FileSink(Path(output_filename)))
