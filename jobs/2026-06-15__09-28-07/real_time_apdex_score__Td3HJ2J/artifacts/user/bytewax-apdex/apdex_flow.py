import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import TumblingWindower, EventClock, fold_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("apdex_flow")

stream = op.input("input", flow, FileSource("input.jsonl"))

def is_valid_line(line):
    return bool(line.strip())

valid_stream = op.filter("filter_empty", stream, is_valid_line)

def parse_json(line):
    data = json.loads(line)
    return (data["service"], data)

parsed = op.map("parse_json", valid_stream, parse_json)

def extract_timestamp(v):
    return datetime.fromisoformat(v["timestamp"].replace("Z", "+00:00"))

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
)

def build_apdex():
    return (0, 0, 0)

def fold_apdex(acc, v):
    satisfied, tolerating, total = acc
    rt = v["response_time_ms"]
    if rt <= 500:
        satisfied += 1
    elif rt <= 2000:
        tolerating += 1
    total += 1
    return (satisfied, tolerating, total)

def merge_apdex(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

windowed = fold_window(
    "fold_apdex",
    parsed,
    clock,
    windower,
    builder=build_apdex,
    folder=fold_apdex,
    merger=merge_apdex,
)

def rekey_down(item):
    key, (window_id, acc) = item
    return (f"{key}_{window_id}", (key, acc))

def rekey_meta(item):
    key, (window_id, meta) = item
    return (f"{key}_{window_id}", meta)

down_rekeyed = op.map("rekey_down", windowed.down, rekey_down)
meta_rekeyed = op.map("rekey_meta", windowed.meta, rekey_meta)

joined = op.join("join_meta", down_rekeyed, meta_rekeyed)

def format_output(item):
    join_key, ((key, (satisfied, tolerating, total)), meta) = item
    apdex_score = (satisfied + (tolerating / 2.0)) / total if total > 0 else 0.0
    window_start = meta.open_time.isoformat().replace("+00:00", "Z")
    
    out_dict = {
        "window_start": window_start,
        "service": key,
        "apdex_score": round(apdex_score, 2)
    }
    return (key, json.dumps(out_dict))

out_stream = op.map("format_output", joined, format_output)

op.output("out", out_stream, FileSink(Path("output.jsonl")))

if __name__ == "__main__":
    run_main(flow)
