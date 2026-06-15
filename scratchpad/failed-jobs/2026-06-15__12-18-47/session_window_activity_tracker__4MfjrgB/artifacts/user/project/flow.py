import json
from datetime import datetime, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource, FileSink

def parse_event(line):
    event = json.loads(line)
    return (event["user_id"], event)

def extract_timestamp(event):
    ts_str = event["timestamp"]
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)

def format_output(item):
    user_id, (window_id, events) = item
    event_types = [e["event_type"] for e in events]
    out_json = json.dumps({
        "user_id": user_id,
        "events": event_types
    })
    return (str(user_id), out_json)

flow = Dataflow("session_windowing")

inp = op.input("input", flow, FileSource("/home/user/project/input.jsonl"))

keyed = op.map("parse", inp, parse_event)

clock = win.EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

windower = win.SessionWindower(gap=timedelta(seconds=5))

windowed = win.collect_window("window", keyed, clock, windower)

out = op.map("format", windowed.down, format_output)

op.output("output", out, FileSink("/home/user/project/output.jsonl"))
