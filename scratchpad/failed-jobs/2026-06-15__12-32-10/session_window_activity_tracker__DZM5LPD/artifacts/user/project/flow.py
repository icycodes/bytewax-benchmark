import json
from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op
from bytewax.operators.windowing import SessionWindower, EventClock, collect_window

flow = Dataflow("session_window")

# Read lines from input file
lines = op.input("input", flow, FileSource("/home/user/project/input.jsonl"))

# Parse each line as JSON
def parse_line(line):
    return json.loads(line)

events = op.map("parse", lines, parse_line)

# Key by user_id for windowing
keyed = op.key_on("key_on", events, lambda e: e["user_id"])

# Set up EventClock using the event timestamp
def get_timestamp(event):
    return datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

clock = EventClock(
    ts_getter=get_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Session window with 5-second gap
windower = SessionWindower(gap=timedelta(seconds=5))

# Collect events in each session window
windowed = collect_window("collect_window", keyed, clock, windower)

# Extract event_types from collected event dicts
def format_output(item):
    user_id, (window_id, event_dicts) = item
    events_list = [e["event_type"] for e in event_dicts]
    return (user_id, json.dumps({"user_id": user_id, "events": events_list}))

output_stream = op.map("format_output", windowed.down, format_output)

# Write to output file
op.output("output", output_stream, FileSink("/home/user/project/output.jsonl"))