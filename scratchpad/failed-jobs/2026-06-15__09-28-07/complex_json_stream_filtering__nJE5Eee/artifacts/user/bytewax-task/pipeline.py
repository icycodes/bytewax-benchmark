import os
import json
from pathlib import Path
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op

run_id = os.environ.get("ZEALT_RUN_ID", "default")
input_file = Path("input.jsonl")
output_file = Path(f"output-{run_id}.jsonl")

def parse_json(line):
    try:
        data = json.loads(line)
        if "type" in data and "device_id" in data and "payload" in data:
            data["device_id"] = str(data["device_id"])
            return data
    except Exception:
        pass
    return None

def key_by_device(data):
    return data["device_id"], data

def process_event(state, event):
    if state is None:
        state = 100.0  # Default threshold
    
    alert = None
    if event["type"] == "config":
        if "threshold" in event["payload"]:
            try:
                state = float(event["payload"]["threshold"])
            except ValueError:
                pass
    elif event["type"] == "metric":
        if "temperature" in event["payload"]:
            try:
                temp = float(event["payload"]["temperature"])
                if temp > state:
                    alert = {
                        "device_id": event["device_id"],
                        "alert_type": "temperature_high",
                        "value": temp,
                        "threshold": state
                    }
            except ValueError:
                pass
    return state, alert

def filter_alerts(key_alert):
    return key_alert[1] is not None

def format_alert(key_alert):
    key, alert = key_alert
    return key, json.dumps(alert)

flow = Dataflow("device_alerts")

stream = op.input("read_file", flow, FileSource(input_file))
parsed_stream = op.filter_map("parse_json", stream, parse_json)
keyed_stream = op.map("key_by_device", parsed_stream, key_by_device)
processed_stream = op.stateful_map("process_event", keyed_stream, process_event)
alerts_stream = op.filter("filter_alerts", processed_stream, filter_alerts)
json_alerts_stream = op.map("format_alert", alerts_stream, format_alert)
op.output("write_file", json_alerts_stream, FileSink(output_file))
