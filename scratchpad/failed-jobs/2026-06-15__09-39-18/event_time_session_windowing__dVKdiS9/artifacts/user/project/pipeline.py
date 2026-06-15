import json
from datetime import timedelta, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import input as op_input, map as op_map, output as op_output
from bytewax.operators.windowing import EventClock, SessionWindower, count_window


def parse_event(line: str):
    """Parse a JSON line into a dict with a timezone-aware datetime timestamp."""
    event = json.loads(line)
    # Ensure timestamp is parsed; fromisoformat handles ISO 8601 including 'Z' in Python 3.11+
    return event


def extract_timestamp(event):
    """Extract timezone-aware datetime from event for EventClock."""
    from datetime import datetime
    ts_str = event["timestamp"]
    # Replace 'Z' suffix if present for robustness
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts_str)
    # Ensure timezone-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def format_output(item):
    """Format windowed count output as a (key, value) tuple for the sink."""
    user_id, (window_id, total_pages) = item
    return (user_id, json.dumps({"user_id": user_id, "total_pages": total_pages}))


flow = Dataflow("session-clickstream")

# Read input JSONL file
inp = op_input("read_input", flow, FileSource("input.jsonl"))

# Parse each line from JSON string to dict
parsed = op_map("parse_json", inp, parse_event)

# Define event-time clock using timestamps from the events
clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Define session windower with 10-second inactivity gap
windower = SessionWindower(gap=timedelta(seconds=10))

# Count pages per session per user
windowed = count_window(
    "count_pages",
    parsed,
    clock=clock,
    windower=windower,
    key=lambda event: event["user_id"],
)

# Format output: windowed.down is Stream[Tuple[str, Tuple[int, int]]]
# where each element is (user_id, (window_id, total_pages))
result = op_map("format_output", windowed.down, format_output)

# Write output JSONL file
op_output("write_output", result, FileSink("output.jsonl"))