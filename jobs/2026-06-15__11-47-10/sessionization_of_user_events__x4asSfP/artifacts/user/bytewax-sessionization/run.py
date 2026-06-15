import os
import json
from datetime import datetime, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def main():
    # Get the directory of the current script
    dir_path = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir_path, "user_events.jsonl")
    output_path = os.path.join(dir_path, "sessions.jsonl")

    # Instantiate dataflow
    flow = Dataflow("sessionization")

    # Read events line-by-line
    up = op.input("input", flow, FileSource(input_path))

    # Parse JSON line and extract user_id as key, event payload as value
    def parse_and_key(line):
        event = json.loads(line)
        return (event["user_id"], event)

    keyed_events = op.map("parse_and_key", up, parse_and_key)

    # Extract timestamp from event payload
    def extract_timestamp(event):
        return datetime.fromisoformat(event["timestamp"])

    # Configure EventClock and SessionWindower
    clock = EventClock(
        ts_getter=extract_timestamp,
        wait_for_system_duration=timedelta(seconds=0)
    )
    windower = SessionWindower(gap=timedelta(minutes=30))

    # Group events into sessions
    windowed = collect_window("collect_sessions", keyed_events, clock, windower)

    # Summarize each session
    def summarize_session(item):
        user_id, (window_id, events) = item
        
        # Parse timestamps to calculate start and end times
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
        session_start = min(timestamps).isoformat().replace("+00:00", "Z")
        session_end = max(timestamps).isoformat().replace("+00:00", "Z")
        event_count = len(events)
        
        result = {
            "user_id": user_id,
            "session_start": session_start,
            "session_end": session_end,
            "event_count": event_count
        }
        return (user_id, json.dumps(result))

    sessions = op.map("summarize_session", windowed.down, summarize_session)

    # Write the results to sessions.jsonl
    op.output("output", sessions, FileSink(output_path))

    # Run the dataflow
    run_main(flow)

if __name__ == "__main__":
    main()
