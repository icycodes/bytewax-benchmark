from datetime import datetime, timedelta, timezone
import json
import statistics

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SlidingWindower, collect_window
from bytewax.connectors.files import FileSource, FileSink

flow = Dataflow("anomaly_detection")

# Read from file
stream = op.input("input", flow, FileSource("/home/user/anomaly_detection/data.jsonl"))

# Parse JSON
def parse_json(line):
    return json.loads(line)

parsed = op.map("parse", stream, parse_json)

# Group by sensor_id -> returns (sensor_id, dict)
def key_by_sensor(data):
    return data["sensor_id"]

keyed = op.key_on("key_on_sensor", parsed, key_by_sensor)

# Setup windowing
def extract_timestamp(data):
    # e.g. "2026-01-01T12:00:00Z"
    return datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=20),
    align_to=datetime(2026, 1, 1, tzinfo=timezone.utc)
)

# Collect window
windowed = collect_window("window", keyed, clock, windower)

# Process window to find outliers
def find_outliers(item):
    # item is (sensor_id, (window_id, elements))
    sensor_id, (window_id, elements) = item
    
    if not elements:
        return []
        
    values = [e["value"] for e in elements]
    mean = statistics.mean(values)
    std_dev = statistics.stdev(values) if len(values) > 1 else 0.0
    
    outliers = []
    if std_dev > 0:
        for e in elements:
            if e["value"] > mean + 3 * std_dev or e["value"] < mean - 3 * std_dev:
                outlier_data = dict(e)
                outlier_data["mean"] = mean
                outlier_data["std_dev"] = std_dev
                outliers.append(outlier_data)
                
    return outliers

outliers = op.flat_map("find_outliers", windowed.down, find_outliers)

# Format to JSON
def format_outlier(outlier):
    return (outlier["sensor_id"], json.dumps(outlier))

formatted = op.map("format_outlier", outliers, format_outlier)

# Sink to file
from pathlib import Path
op.output("output", formatted, FileSink(Path("/home/user/anomaly_detection/anomalies.jsonl")))
