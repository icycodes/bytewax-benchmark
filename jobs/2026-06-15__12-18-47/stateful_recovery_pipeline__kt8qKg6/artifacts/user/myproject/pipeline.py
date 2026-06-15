import json
import math
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import SlidingWindower, EventClock, fold_window
from bytewax.connectors.files import FileSource, FileSink

def parse_input(line: str):
    data = json.loads(line)
    # Parse ISO8601 string into timezone-aware datetime
    data["time"] = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))
    return (data["sensor_id"], data)

def extract_time(data):
    return data["time"]

def builder():
    # count, sum, sum_sq
    return (0, 0.0, 0.0)

def folder(acc, data):
    count, sum_x, sum_x2 = acc
    val = data["temp"]
    return (count + 1, sum_x + val, sum_x2 + val * val)

def merger(acc1, acc2):
    return (
        acc1[0] + acc2[0],
        acc1[1] + acc2[1],
        acc1[2] + acc2[2]
    )

def format_output(key_item):
    key, (win_id, acc) = key_item
    count, sum_x, sum_x2 = acc
    
    # Calculate stats
    mean = sum_x / count if count > 0 else 0.0
    variance = (sum_x2 - (sum_x ** 2) / count) / count if count > 0 else 0.0
    if variance < 0:
        variance = 0.0
    stddev = math.sqrt(variance)
    
    # Calculate window start and end
    align_to = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    offset = timedelta(seconds=30)
    length = timedelta(seconds=60)
    
    window_start = align_to + offset * win_id
    window_end = window_start + length
    
    out_dict = {
        "sensor_id": key,
        "window_start": window_start.isoformat().replace("+00:00", "Z"),
        "window_end": window_end.isoformat().replace("+00:00", "Z"),
        "mean": mean,
        "stddev": stddev
    }
    return (key, json.dumps(out_dict))

flow = Dataflow("sliding_window_outlier_detector")

# Input
inp = op.input("inp", flow, FileSource("input.jsonl"))

# Parse and key
keyed = op.map("parse", inp, parse_input)

# Windowing
clock = EventClock(
    ts_getter=extract_time,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
)

out = fold_window(
    "win",
    keyed,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger
)

# Format output
formatted = op.map("format", out.down, format_output)

# Output
op.output("out", formatted, FileSink("output.jsonl"))
