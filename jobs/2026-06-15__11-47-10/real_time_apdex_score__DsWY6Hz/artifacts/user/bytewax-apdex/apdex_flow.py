import json
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import TumblingWindower, EventClock, fold_window
from bytewax.testing import run_main

# 1. Initialize the Dataflow
flow = Dataflow("apdex_calculation")

# 2. Input from input.jsonl
input_path = "input.jsonl"
output_path = "output.jsonl"

up = op.input("input", flow, FileSource(input_path))

# 3. Parse JSON lines robustly
def parse_json(line):
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None

parsed = op.filter_map("parse_json", up, parse_json)

# 4. Key by service
keyed = op.key_on("key_by_service", parsed, lambda x: x["service"])

# 5. Define clock and windower
def extract_timestamp(item):
    dt = datetime.fromisoformat(item["timestamp"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Align windows to start of epoch/reference time
align_to = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=align_to,
)

# 6. Fold window to aggregate response times
def builder():
    return {
        "satisfied": 0,
        "tolerating": 0,
        "frustrated": 0,
        "total": 0
    }

def folder(acc, item):
    rt = item["response_time_ms"]
    acc["total"] += 1
    if rt <= 500:
        acc["satisfied"] += 1
    elif rt <= 2000:
        acc["tolerating"] += 1
    else:
        acc["frustrated"] += 1
    return acc

def merger(acc1, acc2):
    return {
        "satisfied": acc1["satisfied"] + acc2["satisfied"],
        "tolerating": acc1["tolerating"] + acc2["tolerating"],
        "frustrated": acc1["frustrated"] + acc2["frustrated"],
        "total": acc1["total"] + acc2["total"]
    }

windowed = fold_window(
    "fold_window",
    keyed,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger,
)

# 7. Map down and meta streams to have string keys for joining
def map_down(item):
    service, (window_id, acc) = item
    new_key = f"{service}@{window_id}"
    return (new_key, (service, window_id, acc))

def map_meta(item):
    service, (window_id, metadata) = item
    new_key = f"{service}@{window_id}"
    return (new_key, metadata)

mapped_down = op.map("map_down", windowed.down, map_down)
mapped_meta = op.map("map_meta", windowed.meta, map_meta)

# 8. Join the mapped streams
joined = op.join("join_meta", mapped_down, mapped_meta)

# 9. Format output to JSON string
def format_output(item):
    new_key, ((service, window_id, acc), metadata) = item
    total = acc["total"]
    if total > 0:
        apdex = (acc["satisfied"] + (acc["tolerating"] / 2.0)) / total
    else:
        apdex = 0.0
    apdex_score = round(apdex, 2)
    
    # Format window_start to ISO 8601 string ending with Z
    window_start_str = metadata.open_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    out_dict = {
        "window_start": window_start_str,
        "service": service,
        "apdex_score": apdex_score
    }
    return (service, json.dumps(out_dict))

formatted = op.map("format_output", joined, format_output)

# 10. Output to output.jsonl
op.output("output", formatted, FileSink(output_path))

if __name__ == "__main__":
    run_main(flow)
