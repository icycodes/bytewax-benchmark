from datetime import datetime, timedelta
import json
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

# Create the dataflow
flow = Dataflow("sessionization")

# Read from user_events.jsonl
input_stream = op.input("input", flow, FileSource("user_events.jsonl"))

# Parse JSON and add datetime timestamp
def parse_event(line):
    event = json.loads(line)
    event["timestamp_dt"] = datetime.fromisoformat(event["timestamp"])
    return event

parsed_stream = op.map("parse_event", input_stream, parse_event)

# Key the stream by user_id
keyed_stream = op.key_on("key_on_user_id", parsed_stream, lambda event: event["user_id"])

# Define EventClock and SessionWindower
def extract_timestamp(event):
    return event["timestamp_dt"]

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

windower = SessionWindower(gap=timedelta(minutes=30))

# Collect events into session windows
windowed_stream = collect_window("session_window", keyed_stream, clock, windower)

# Format sessions into JSON strings (and key them for FileSink)
def format_session(item):
    user_id, (window_id, values) = item
    first_event = values[0]
    last_event = values[-1]
    
    session_start = first_event["timestamp_dt"].isoformat().replace("+00:00", "Z")
    session_end = last_event["timestamp_dt"].isoformat().replace("+00:00", "Z")
    
    session_data = {
        "user_id": user_id,
        "session_start": session_start,
        "session_end": session_end,
        "event_count": len(values)
    }
    return user_id, json.dumps(session_data)

formatted_stream = op.map("format_session", windowed_stream.down, format_session)

# Write to sessions.jsonl
op.output("output", formatted_stream, FileSink(Path("sessions.jsonl")))

if __name__ == "__main__":
    run_main(flow)
