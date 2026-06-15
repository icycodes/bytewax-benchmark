import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.run import cli_main


INPUT_PATH = Path(__file__).parent / "user_events.jsonl"
OUTPUT_PATH = Path(__file__).parent / "sessions.jsonl"

# FileSink requires the file to exist before writing
OUTPUT_PATH.touch()


def parse_event(line: str) -> dict:
    """Parse a JSON line into an event dict."""
    return json.loads(line)


def get_timestamp(event: dict) -> datetime:
    """Extract the event timestamp as a UTC-aware datetime."""
    ts = event["timestamp"]
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def summarize_session_value(window_id_and_events) -> str:
    """
    Receive the *value* side of the windowed keyed stream:
      (window_id, [event, ...])

    Returns a JSON string.  The key (user_id) is injected by the
    surrounding map_value call which keeps the key in scope via a
    closure — but map_value only hands us the value.  We therefore
    carry user_id inside each event dict (it is already there from
    the source JSON).
    """
    _window_id, events = window_id_and_events
    timestamps = [get_timestamp(e) for e in events]
    session_start = min(timestamps)
    session_end = max(timestamps)
    # user_id is available in every event dict
    user_id = events[0]["user_id"]
    record = {
        "user_id": user_id,
        "session_start": session_start.isoformat(),
        "session_end": session_end.isoformat(),
        "event_count": len(events),
    }
    return json.dumps(record)


# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------
flow = Dataflow("sessionization")

# 1. Read raw lines from the JSONL file
raw = op.input("read_file", flow, FileSource(INPUT_PATH))

# 2. Parse each line into an event dict
events = op.map("parse_event", raw, parse_event)

# 3. Key events by user_id for per-user windowing
keyed = op.key_on("key_by_user", events, lambda e: e["user_id"])

# 4. Event-time clock — wait_for_system_duration=0 because this is a
#    bounded batch job; EOF will flush all open windows immediately.
clock = EventClock(
    ts_getter=get_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# 5. Session windower: a new session starts after 30 minutes of inactivity
windower = SessionWindower(gap=timedelta(minutes=30))

# 6. Collect each session's events into a list
#    windowed.down is a KeyedStream of (user_id, (window_id, [events]))
windowed = collect_window("session_window", keyed, clock, windower)

# 7. Transform the value in-place (stays keyed) → (user_id, json_string)
json_lines = op.map_value("to_json", windowed.down, summarize_session_value)

# 8. Write results to sessions.jsonl
#    FileSink is a FixedPartitionedSink that expects (key, str) tuples
op.output("write_file", json_lines, FileSink(OUTPUT_PATH))


if __name__ == "__main__":
    cli_main(flow)
