import json
from datetime import datetime, timedelta
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import SessionWindower, EventClock, collect_window

# Initialize Dataflow
flow = Dataflow("user_session_flow")

# Read lines from input.jsonl
input_stream = op.input("input", flow, FileSource("/home/user/project/input.jsonl"))

# Parse each JSON line into a dictionary
def parse_json(line):
    return json.loads(line)

parsed = op.map("parse_json", input_stream, parse_json)

# Map the input to a tuple of (user_id, event_dict) so it can be keyed
def key_by_user(event_dict):
    return (event_dict["user_id"], event_dict)

keyed = op.map("key_by_user", parsed, key_by_user)

# Define EventClock based on event timestamp
def extract_timestamp(event_dict):
    return datetime.fromisoformat(event_dict["timestamp"])

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Define SessionWindower with 5 seconds gap
windower = SessionWindower(gap=timedelta(seconds=5))

# Group events into sessions
windowed = collect_window("collect_window", keyed, clock, windower)

# Map the (key, (window_id, items)) tuple into the required output dictionary format
def format_session(item):
    user_id, (window_id, items) = item
    events = [event["event_type"] for event in items]
    return (user_id, json.dumps({
        "user_id": user_id,
        "events": events
    }))

formatted = op.map("format_session", windowed.down, format_session)

# Write output to output.jsonl
op.output("output", formatted, FileSink(Path("/home/user/project/output.jsonl")))
