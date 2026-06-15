import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window

BASE_DIR = Path(__file__).parent

flow = Dataflow("sessionization")

# Read input events from JSON lines file
inp = op.input("input", flow, FileSource(str(BASE_DIR / "user_events.jsonl")))


# Parse each JSON line and add a parsed datetime field
def parse_line(line):
    event = json.loads(line)
    event["ts"] = datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )
    return event


parsed = op.map("parse", inp, parse_line)

# Key the stream by user_id
keyed = op.key_on("key_on", parsed, lambda e: e["user_id"])

# Use event time as the clock
clock = EventClock(
    ts_getter=lambda e: e["ts"],
    wait_for_system_duration=timedelta(seconds=0),
)

# Session window with a 30-minute inactivity gap
windower = SessionWindower(gap=timedelta(minutes=30))

# Collect events per session window
windowed = collect_window("collect_window", keyed, clock, windower)


# Calculate session metrics from collected events
# Returns (key, json_string) tuple for output routing
def calculate_session(item):
    user_id, (window_id, events) = item
    # Events are ordered by timestamp within the window (ordered=True default)
    session_start = events[0]["ts"]
    session_end = events[-1]["ts"]
    event_count = len(events)
    result = json.dumps(
        {
            "user_id": user_id,
            "session_start": session_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "session_end": session_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "event_count": event_count,
        }
    )
    return (user_id, result)


result = op.map("calculate_session", windowed.down, calculate_session)

# Write output to JSON lines file
op.output("output", result, FileSink(BASE_DIR / "sessions.jsonl"))

if __name__ == "__main__":
    from bytewax.testing import run_main
    run_main(flow)