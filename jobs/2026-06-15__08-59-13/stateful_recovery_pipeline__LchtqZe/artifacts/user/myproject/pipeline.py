from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import statistics

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import SlidingWindower, EventClock, fold_window

# Define the dataflow
flow = Dataflow("sensor_dataflow")

# Define input and output paths
input_path = Path("input.jsonl")
output_path = Path("output.jsonl")

# 1. Read input stream
up = op.input("input", flow, FileSource(input_path))

# 2. Parse JSON lines
def parse_json(line):
    return json.loads(line)

parsed = op.map("parse_json", up, parse_json)

# 3. Key the stream by sensor_id
def get_sensor_id(x):
    return x["sensor_id"]

keyed = op.key_on("key_on", parsed, get_sensor_id)

# 4. Define EventClock and SlidingWindower
def extract_timestamp(x):
    # Parse ISO8601 string to timezone-aware datetime in UTC
    dt = datetime.fromisoformat(x["time"])
    return dt.astimezone(timezone.utc)

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
)

# 5. Window folding logic
def builder():
    return []

def folder(acc, x):
    acc.append(x["temp"])
    return acc

def merger(a, b):
    return a + b

windowed = fold_window(
    "fold_window",
    keyed,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger,
)

# 6. Map window results to format and join them
def map_down(x):
    sensor_id, (window_id, accumulator) = x
    return (f"{sensor_id}-{window_id}", (sensor_id, window_id, accumulator))

def map_meta(x):
    sensor_id, (window_id, metadata) = x
    return (f"{sensor_id}-{window_id}", metadata)

down_keyed = op.map("down_keyed", windowed.down, map_down)
meta_keyed = op.map("meta_keyed", windowed.meta, map_meta)

joined = op.join("join", down_keyed, meta_keyed)

# 7. Format the output
def format_utc_datetime(dt):
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def calculate_stats(temps):
    if not temps:
        return 0.0, 0.0
    mean = sum(temps) / len(temps)
    stddev = statistics.pstdev(temps)
    return mean, stddev

def format_output(x):
    join_key, ((sensor_id, window_id, accumulator), metadata) = x
    mean, stddev = calculate_stats(accumulator)
    window_start = format_utc_datetime(metadata.open_time)
    window_end = format_utc_datetime(metadata.close_time)
    
    out_dict = {
        "sensor_id": sensor_id,
        "window_start": window_start,
        "window_end": window_end,
        "mean": mean,
        "stddev": stddev
    }
    return (str(output_path), json.dumps(out_dict))

output_stream = op.map("format_output", joined, format_output)

# 8. Output to FileSink
op.output("output", output_stream, FileSink(output_path))
