import json
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import SessionWindower, EventClock, collect_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.run import cli_main

def parse_event(line):
    event = json.loads(line)
    return event["user_id"], event

def extract_timestamp(event):
    # Parse ISO 8601 string to datetime
    return datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

def format_session(item):
    user_id, (window_id, events) = item
    
    # events is a list of event dicts
    # they are ordered by timestamp because collect_window ordered=True by default
    start_time = events[0]["timestamp"]
    end_time = events[-1]["timestamp"]
    
    session = {
        "user_id": user_id,
        "session_start": start_time,
        "session_end": end_time,
        "event_count": len(events)
    }
    return (user_id, json.dumps(session))

flow = Dataflow("sessionization")

# Read from file
lines = op.input("input", flow, FileSource("user_events.jsonl"))

# Parse json and key by user_id
keyed_events = op.map("parse", lines, parse_event)

# Define clock and windower
clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SessionWindower(gap=timedelta(minutes=30))

# Collect window
windowed = collect_window("collect_window", keyed_events, clock, windower)

# Format the output
formatted = op.map("format_session", windowed.down, format_session)

# Write to file
op.output("output", formatted, FileSink("sessions.jsonl"))

if __name__ == "__main__":
    cli_main(flow)
