import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax._bytewax import cli_main
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(BASE_DIR, "user_events.jsonl")
OUTPUT_PATH = os.path.join(BASE_DIR, "sessions.jsonl")

# FileSink requires the output file to exist before the dataflow runs.
Path(OUTPUT_PATH).touch()

# ---------------------------------------------------------------------------
# Helper: parse an ISO 8601 timestamp string into an aware UTC datetime
# ---------------------------------------------------------------------------
def parse_ts(ts_str: str) -> datetime:
    """Parse an ISO 8601 string (with optional trailing 'Z') into a UTC datetime."""
    # Python < 3.11 fromisoformat doesn't handle the trailing 'Z'
    ts_str = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------
flow = Dataflow("sessionization")

# 1. Read raw lines from the JSONL input file
raw = op.input("input", flow, FileSource(INPUT_PATH))

# 2. Parse each JSON line and emit (user_id, event_dict) pairs.
#    A parsed datetime is stored under "_dt" so the EventClock can extract it.
def parse_line(line: str):
    event = json.loads(line)
    event["_dt"] = parse_ts(event["timestamp"])
    return (event["user_id"], event)

keyed = op.map("parse", raw, parse_line)

# 3. Define the event-time clock.
#    ts_getter receives the *value* side of each (key, value) pair — the event dict.
#    wait_for_system_duration=0 is appropriate for a bounded / batch pipeline:
#    the watermark advances to EOF and flushes all open windows at stream end.
clock = EventClock(
    ts_getter=lambda event: event["_dt"],
    wait_for_system_duration=timedelta(seconds=0),
)

# 4. Session windower: a new session starts after 30 minutes of inactivity.
windower = SessionWindower(gap=timedelta(minutes=30))

# 5. Collect every event in a session into an ordered list.
#    windowed.down yields: (user_id, (window_id, [event, ...]))
windowed = collect_window("session_window", keyed, clock, windower)

# 6. Summarise each completed session window.
#    FileSink expects (key, value) 2-tuples where value is the string to write.
def summarize_session(item):
    user_id, (_window_id, events) = item
    # Events are already in timestamp order (collect_window ordered=True by default)
    timestamps = [e["_dt"] for e in events]
    session_start = min(timestamps)
    session_end = max(timestamps)
    line = json.dumps(
        {
            "user_id": user_id,
            "session_start": session_start.isoformat(),
            "session_end": session_end.isoformat(),
            "event_count": len(events),
        }
    )
    return (user_id, line)

sessions = op.map("summarize", windowed.down, summarize_session)

# 7. Write one JSON object per line to the output file.
op.output("output", sessions, FileSink(OUTPUT_PATH))

# ---------------------------------------------------------------------------
# Entry point: run the dataflow when this script is executed directly.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cli_main(flow)
