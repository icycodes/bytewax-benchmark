import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import EventClock, SlidingWindower, collect_window

# 1. Define helper functions at the module level for pickling/recovery safety

def parse_json(line):
    return json.loads(line)

def get_sensor_id(item):
    return item["sensor_id"]

def extract_timestamp(item):
    ts_str = item["timestamp"]
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)

def detect_anomalies(item):
    sensor_id, (window_id, readings) = item
    if not readings:
        return []
    
    values = [r["value"] for r in readings]
    n = len(values)
    mean = sum(values) / n
    
    # Calculate population standard deviation
    variance = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(variance)
    
    # If standard deviation is 0, there are no outliers
    if std_dev == 0.0:
        return []
    
    outliers = []
    for r in readings:
        val = r["value"]
        if val > mean + 3 * std_dev or val < mean - 3 * std_dev:
            outlier_record = {
                "sensor_id": sensor_id,
                "timestamp": r["timestamp"],
                "value": val,
                "mean": mean,
                "std_dev": std_dev
            }
            outliers.append(outlier_record)
            
    return outliers

def format_for_sink(item):
    return (item["sensor_id"], json.dumps(item))


# 2. Build the Bytewax Dataflow

flow = Dataflow("anomaly_detection")

# Input step: read lines from the JSONL file
input_path = "/home/user/anomaly_detection/data.jsonl"
lines = op.input("input", flow, FileSource(input_path))

# Parse step: deserialize JSON lines
parsed = op.map("parse", lines, parse_json)

# Key step: group by sensor_id
keyed = op.key_on("key_on", parsed, get_sensor_id)

# Windowing configuration
clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=20),
    align_to=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
)

# Collect window step
windowed = collect_window("collect_window", keyed, clock, windower)

# Anomaly detection step
anomalies = op.flat_map("detect_anomalies", windowed.down, detect_anomalies)

# Formatting step for FileSink
formatted = op.map("format_for_sink", anomalies, format_for_sink)

# Output step: write to the output JSONL file
output_path = Path("/home/user/anomaly_detection/anomalies.jsonl")
sink = FileSink(output_path)
op.output("output", formatted, sink)
