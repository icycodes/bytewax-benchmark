import json
from datetime import datetime, timezone, timedelta
import math

from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import EventClock, SlidingWindower

def parse_input(line: str):
    data = json.loads(line)
    return (data["sensor_id"], data)

def extract_time(item):
    return datetime.fromisoformat(item["time"].replace("Z", "+00:00"))

def builder():
    return []

def folder(acc, item):
    acc.append(item["temp"])
    return acc

def merger(acc1, acc2):
    acc1.extend(acc2)
    return acc1

align_to = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
offset = timedelta(seconds=30)
length = timedelta(seconds=60)

def format_output(item):
    # item is (sensor_id, (window_id, temps))
    sensor_id, (window_id, temps) = item
    
    n = len(temps)
    if n == 0:
        mean = 0.0
        stddev = 0.0
    else:
        mean = sum(temps) / n
        variance = sum((x - mean) ** 2 for x in temps) / n
        stddev = math.sqrt(variance)
        
    open_time = align_to + window_id * offset
    close_time = open_time + length
        
    start_str = open_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = close_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    out = {
        "sensor_id": sensor_id,
        "window_start": start_str,
        "window_end": end_str,
        "mean": mean,
        "stddev": stddev
    }
    return (sensor_id, json.dumps(out))

flow = Dataflow("pipeline")

inp = op.input("in", flow, FileSource("input.jsonl"))
parsed = op.map("parse", inp, parse_input)

clock = EventClock(extract_time, wait_for_system_duration=timedelta(seconds=0))

windower = SlidingWindower(
    length=length,
    offset=offset,
    align_to=align_to
)

windowed = win.fold_window("window", parsed, clock, windower, builder, folder, merger)

formatted = op.map("format", windowed.down, format_output)

op.output("out", formatted, FileSink("output.jsonl"))
