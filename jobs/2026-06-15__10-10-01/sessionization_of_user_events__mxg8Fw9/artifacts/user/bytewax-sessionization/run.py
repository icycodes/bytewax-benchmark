import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, collect_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

BASE_DIR = Path(__file__).parent
INPUT_PATH = BASE_DIR / "user_events.jsonl"
OUTPUT_PATH = BASE_DIR / "sessions.jsonl"


def parse_event(line: str) -> dict:
    """Parse a JSON line into a dict, adding a parsed datetime."""
    event = json.loads(line)
    event["_parsed_ts"] = datetime.fromisoformat(event["timestamp"])
    return event


def session_to_output(session_tuple):
    """Convert a collected session window into the output format.

    The input is a tuple: ((user_id, window_id), (window_metadata, events_list))
    where events_list is the list of events collected in the window.
    """
    (user_id, _window_id), (window_meta, events) = session_tuple

    if not events:
        return None

    # Sort events by timestamp to find start/end
    sorted_events = sorted(events, key=lambda e: e["_parsed_ts"])

    session_start = sorted_events[0]["timestamp"]
    session_end = sorted_events[-1]["timestamp"]
    event_count = len(sorted_events)

    return {
        "user_id": user_id,
        "session_start": session_start,
        "session_end": session_end,
        "event_count": event_count,
    }


def build_dataflow():
    flow = Dataflow("sessionization")

    # 1. Read JSON lines from file
    lines = op.input("input", flow, FileSource(INPUT_PATH))

    # 2. Parse JSON lines into event dicts
    events = op.map("parse", lines, parse_event)

    # 3. Key by user_id
    keyed = op.key_on("key_on_user", events, lambda e: e["user_id"])

    # 4. Define event-time clock (extract parsed datetime from event)
    def ts_getter(event: dict) -> datetime:
        return event["_parsed_ts"]

    clock = EventClock(
        ts_getter=ts_getter,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 5. Define session windower with 30-minute gap
    windower = SessionWindower(gap=timedelta(minutes=30))

    # 6. Collect events into session windows
    window_out = collect_window(
        "session_window",
        keyed,
        clock,
        windower,
    )

    # 7. Join window metadata with collected events
    joined = op.join("join_meta", window_out.down, window_out.meta)

    # 8. Map to output format
    output_stream = op.map("format_output", joined, session_to_output)

    # 9. Serialize to JSON lines
    json_lines = op.map("to_json", output_stream, json.dumps)

    # 10. Write to output file
    op.output("output", json_lines, FileSink(OUTPUT_PATH))

    return flow


if __name__ == "__main__":
    flow = build_dataflow()
    run_main(flow)
