import json
from datetime import datetime, timedelta
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("sessionization")

inp = op.input("input", flow, FileSource("user_events.jsonl"))

def parse_line(line):
    return json.loads(line)

parsed = op.map("parse", inp, parse_line)

keyed = op.key_on("key_on", parsed, lambda x: x["user_id"])

def get_event_time(event):
    return datetime.fromisoformat(event["timestamp"])

clock = wop.EventClock(
    ts_getter=get_event_time,
    wait_for_system_duration=timedelta(seconds=0)
)

windower = wop.SessionWindower(gap=timedelta(minutes=30))

windowed = wop.collect_window("session_window", keyed, clock, windower)

def format_session(item):
    user_id, (window_id, events) = item
    
    start_time = events[0]["timestamp"]
    end_time = events[-1]["timestamp"]
    
    session = {
        "user_id": user_id,
        "session_start": start_time,
        "session_end": end_time,
        "event_count": len(events)
    }
    return (user_id, json.dumps(session))

formatted = op.map("format_session", windowed.down, format_session)

op.output("output", formatted, FileSink(Path("sessions.jsonl")))

if __name__ == "__main__":
    run_main(flow)
