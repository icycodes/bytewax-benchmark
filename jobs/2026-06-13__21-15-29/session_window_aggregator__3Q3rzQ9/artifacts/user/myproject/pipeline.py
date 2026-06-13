import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import SessionWindower, EventClock, collect_window
from bytewax.connectors.files import FileSource, FileSink

# Create or truncate the output file as required by FileSink
output_path = Path("/home/user/myproject/output.jsonl")
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text("")

input_path = Path("/home/user/myproject/input.jsonl")

# Define the dataflow
flow = Dataflow("session_aggregator")

def parse_and_key(line_str):
    line_str = line_str.strip()
    if not line_str:
        return None
    try:
        data = json.loads(line_str)
        user_id = str(data["user_id"])
        page = data["page"]
        ts_str = data["timestamp"]
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return (user_id, {"user_id": user_id, "page": page, "timestamp": dt})
    except Exception as e:
        return None

# Parse input lines and filter out any invalid/None values
keyed_stream = op.filter_map("parse_input", op.input("input", flow, FileSource(input_path)), parse_and_key)

def extract_timestamp(item):
    return item["timestamp"]

# Use EventClock with a large wait_for_system_duration to handle out of order events deterministically in batch mode
clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(days=365)
)

# Session closes after 10 seconds of inactivity
windower = SessionWindower(gap=timedelta(seconds=10))

# Collect session items
windowed = collect_window("session_window", keyed_stream, clock, windower)

def format_session(item):
    user_id, (window_id, items) = item
    # Since collect_window(ordered=True) is used, items is already sorted by event time.
    pages = [x["page"] for x in items]
    output_obj = {
        "user_id": user_id,
        "page_count": len(pages),
        "pages": pages
    }
    return (user_id, json.dumps(output_obj))

formatted_stream = op.map("format_output", windowed.down, format_session)

# Write output
op.output("output", formatted_stream, FileSink(output_path))
