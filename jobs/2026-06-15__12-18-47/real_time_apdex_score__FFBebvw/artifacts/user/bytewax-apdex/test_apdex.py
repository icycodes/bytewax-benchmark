import json
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import TumblingWindower, EventClock, fold_window
from bytewax.testing import TestingSource, run_main

flow = Dataflow("apdex")

data = [
    '{"timestamp": "2023-10-01T10:00:00Z", "service": "auth", "response_time_ms": 200}',
    '{"timestamp": "2023-10-01T10:00:05Z", "service": "auth", "response_time_ms": 600}',
    '{"timestamp": "2023-10-01T10:00:15Z", "service": "auth", "response_time_ms": 2500}'
]

up = op.input("input", flow, TestingSource(data))

def parse_json(line):
    return json.loads(line)

parsed = op.map("parse", up, parse_json)

def extract_key(x):
    return x["service"]

keyed = op.key_on("key_on", parsed, extract_key)

def extract_timestamp(x):
    return datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00"))

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

# Tumbling windower with 10 seconds
windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=datetime(2023, 10, 1, 0, 0, tzinfo=timezone.utc)
)

def builder():
    return {"s": 0, "t": 0, "total": 0}

def folder(acc, x):
    rt = x["response_time_ms"]
    acc["total"] += 1
    if rt <= 500:
        acc["s"] += 1
    elif rt <= 2000:
        acc["t"] += 1
    return acc

def merger(a, b):
    return {
        "s": a["s"] + b["s"],
        "t": a["t"] + b["t"],
        "total": a["total"] + b["total"]
    }

windowed = fold_window("fold", keyed, clock, windower, builder, folder, merger)

def rekey_down(x):
    key, (win_id, acc) = x
    return (f"{key}_{win_id}", (key, acc))

def rekey_meta(x):
    key, (win_id, meta) = x
    return (f"{key}_{win_id}", meta)

down_rekeyed = op.map("rekey_down", windowed.down, rekey_down)
meta_rekeyed = op.map("rekey_meta", windowed.meta, rekey_meta)

joined = op.join("join", down_rekeyed, meta_rekeyed)

def format_output(x):
    _, ((service, acc), meta) = x
    score = (acc["s"] + acc["t"] / 2.0) / acc["total"] if acc["total"] > 0 else 0.0
    
    # Format to ISO 8601 with Z
    window_start = meta.open_time.isoformat()
    if window_start.endswith('+00:00'):
        window_start = window_start[:-6] + 'Z'
        
    return json.dumps({
        "window_start": window_start,
        "service": service,
        "apdex_score": round(score, 2)
    })

out = op.map("format", joined, format_output)
op.inspect("out", out)

run_main(flow)
