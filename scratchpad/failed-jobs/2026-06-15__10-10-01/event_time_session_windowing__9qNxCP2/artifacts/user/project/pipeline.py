import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    SessionWindower,
    fold_window,
)
from bytewax.connectors.files import FileSource, FileSink


flow = Dataflow("clickstream_session_aggregator")

# Step 1: Read input JSONL
inp = op.input("read_input", flow, FileSource(Path("input.jsonl")))


# Step 2: Parse JSON lines and extract fields
def parse_event(raw: str) -> tuple:
    """Parse a JSON line into a structured event."""
    obj = json.loads(raw)
    user_id = obj["user_id"]
    page = obj["page"]
    timestamp = datetime.fromisoformat(obj["timestamp"])
    # Ensure timezone-aware
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (user_id, page, timestamp)


parsed = op.map("parse_events", inp, parse_event)


# Step 3: Key the stream by user_id (must be a string key)
keyed = op.key_on("key_by_user", parsed, lambda x: x[0])


# Step 4: Define the event-time clock and session windower
def extract_timestamp(event: tuple) -> datetime:
    """Extract the timestamp from the event tuple."""
    return event[2]


clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SessionWindower(gap=timedelta(seconds=10))


# Step 5: Fold window — count pages per session
def folder(acc: int, event: tuple) -> int:
    """Increment the page count for each event in the window."""
    return acc + 1


windowed = fold_window(
    "session_windows",
    keyed,
    clock,
    windower,
    builder=lambda: 0,
    folder=folder,
    merger=lambda a, b: a + b,
)


# Step 6: Map the window output to the desired format
# windowed.down emits: (user_id, (window_id, total_pages))
def format_output(item: tuple) -> tuple:
    """Format a window result as a keyed JSON line for FileSink."""
    user_id, (_window_id, total_pages) = item
    line = json.dumps({"user_id": user_id, "total_pages": total_pages})
    return (user_id, line)


output_stream = op.map("format_output", windowed.down, format_output)


# Step 7: Write to output.jsonl
op.output("write_output", output_stream, FileSink(Path("output.jsonl")))
