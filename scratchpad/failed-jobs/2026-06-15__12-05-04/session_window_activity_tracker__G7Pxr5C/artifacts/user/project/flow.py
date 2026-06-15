import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_line(line: str) -> tuple[str, dict]:
    """Parse a JSON line into a (user_id, event_dict) keyed tuple."""
    event = json.loads(line)
    return (event["user_id"], event)


def get_event_timestamp(event: dict) -> datetime:
    """Extract the event timestamp as an aware UTC datetime."""
    ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    # Ensure the datetime is timezone-aware in UTC.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def format_output(item: tuple) -> tuple[str, str]:
    """Map (user_id, (window_id, events)) → (key, JSON string) for the sink."""
    user_id, (window_id, events) = item
    record = {
        "user_id": user_id,
        "events": [e["event_type"] for e in events],
    }
    return (user_id, json.dumps(record))


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------

flow = Dataflow("session_windowing")

# 1. Read raw lines from the input file.
lines = op.input("input", flow, FileSource("/home/user/project/input.jsonl"))

# 2. Parse each line into (user_id, event_dict) so the stream is keyed.
keyed_events = op.map("parse", lines, parse_line)

# 3. Define an EventClock that reads the embedded timestamp.
#    wait_for_system_duration=0 is appropriate for a bounded/batch source.
clock = EventClock(
    ts_getter=get_event_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# 4. SessionWindower with a 5-second inactivity gap.
windower = SessionWindower(gap=timedelta(seconds=5))

# 5. Collect all events in each session window into a list.
windowed = collect_window("session_window", keyed_events, clock, windower)

# 6. Map the windowed output to a JSON string.
json_lines = op.map("format", windowed.down, format_output)

# 7. Write JSON lines to the output file.
op.output("output", json_lines, FileSink(Path("/home/user/project/output.jsonl")))
