import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import EventClock, SessionWindower, fold_window

# Define the dataflow
flow = Dataflow("clickstream_aggregator")

# Read lines from input.jsonl
stream = op.input("input", flow, FileSource(Path("input.jsonl")))

# Parse each line as JSON
def parse_line(line):
    return json.loads(line)

parsed_stream = op.map("parse_json", stream, parse_line)

# Key the stream on user_id
keyed_stream = op.key_on("key_by_user", parsed_stream, lambda event: event["user_id"])

# Define the clock to extract event-time from the "timestamp" field
def extract_timestamp(event):
    ts_str = event["timestamp"]
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Define the session windower with a 10-second inactivity gap
windower = SessionWindower(gap=timedelta(seconds=10))

# Fold the window to compute total pages visited per session
windowed = fold_window(
    "session_fold",
    keyed_stream,
    clock,
    windower,
    builder=lambda: 0,
    folder=lambda acc, event: acc + 1,
    merger=lambda a, b: a + b,
)

# Format the windowed output to the expected JSON schema as a (key, value) tuple
def format_output(item):
    user_id, (window_id, total_pages) = item
    out_dict = {
        "user_id": user_id,
        "total_pages": total_pages
    }
    return user_id, json.dumps(out_dict)

formatted_stream = op.map("format_output", windowed.down, format_output)

# Write output to output.jsonl
op.output("output", formatted_stream, FileSink(Path("output.jsonl")))
