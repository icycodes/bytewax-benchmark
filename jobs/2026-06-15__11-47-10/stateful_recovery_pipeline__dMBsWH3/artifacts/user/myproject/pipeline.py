import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    SlidingWindower,
    EventClock,
    fold_window
)
from bytewax.connectors.files import FileSource, FileSink

# Define the dataflow
flow = Dataflow("sensor_outlier_detector")

# 1. Read JSON lines from input.jsonl using FileSource
input_path = "input.jsonl"
stream = op.input("input_step", flow, FileSource(input_path))

# 2. Parse JSON lines and parse time into timezone-aware datetime (UTC)
def parse_and_enrich(line):
    data = json.loads(line)
    time_str = data["time"]
    # Handle 'Z' suffix for Python datetime parsing
    if time_str.endswith("Z"):
        time_str = time_str[:-1] + "+00:00"
    data["datetime"] = datetime.fromisoformat(time_str)
    return data

parsed_stream = op.map("parse_json", stream, parse_and_enrich)

# 3. Key the stream on sensor_id (string)
def key_by_sensor(item):
    return item["sensor_id"]

keyed_stream = op.key_on("key_by_sensor_step", parsed_stream, key_by_sensor)

# 4. Define EventClock and SlidingWindower
def get_item_timestamp(item):
    return item["datetime"]

clock = EventClock(
    ts_getter=get_item_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
)

# 5. Implement builder, folder, and merger for fold_window
# Ensure accumulator state is a pure Python object (list of floats) and functions are named top-level
def builder():
    return []

def folder(acc, item):
    acc.append(item["temp"])
    return acc

def merger(acc1, acc2):
    acc1.extend(acc2)
    return acc1

windowed_stream = fold_window(
    "fold_window_step",
    keyed_stream,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger
)

# 6. Map windowed.down and windowed.meta to a keyed stream with key f"{sensor_id}:{window_id}"
def map_down(item):
    sensor_id, (window_id, acc) = item
    return (f"{sensor_id}:{window_id}", acc)

def map_meta(item):
    sensor_id, (window_id, metadata) = item
    return (f"{sensor_id}:{window_id}", metadata)

down_keyed = op.map("map_down", windowed_stream.down, map_down)
meta_keyed = op.map("map_meta", windowed_stream.meta, map_meta)

# 7. Join the two streams
joined_stream = op.join("join_windows", down_keyed, meta_keyed)

# 8. Map the joined output to the required JSON format
def format_output(item):
    joined_key, (temps, metadata) = item
    sensor_id = joined_key.split(":")[0]
    
    if temps:
        mean = statistics.mean(temps)
        stddev = statistics.pstdev(temps)
    else:
        mean = 0.0
        stddev = 0.0
        
    window_start = metadata.open_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = metadata.close_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    result = {
        "sensor_id": sensor_id,
        "window_start": window_start,
        "window_end": window_end,
        "mean": float(mean),
        "stddev": float(stddev)
    }
    return (sensor_id, json.dumps(result))

formatted_output = op.map("format_output", joined_stream, format_output)

# 9. Write the results to output.jsonl
output_path = Path("output.jsonl")
op.output("output_step", formatted_output, FileSink(output_path))
