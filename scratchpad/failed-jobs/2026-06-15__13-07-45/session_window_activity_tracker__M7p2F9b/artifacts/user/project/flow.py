import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.operators import input as inp, map, output
from bytewax.operators.windowing import (
    EventClock,
    SessionWindower,
    collect_window,
)


def parse_line(line: str) -> dict:
    """Parse a JSON line into a dict and parse the timestamp."""
    event = json.loads(line)
    event["_parsed_ts"] = datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )
    return event


def key_on_user(event: dict) -> tuple:
    """Key the event by user_id."""
    return (event["user_id"], event)


def ts_getter(event: dict) -> datetime:
    """Extract the parsed timestamp from the event."""
    return event["_parsed_ts"]


def format_output(item: tuple) -> tuple:
    """Format a window result into a (key, json_string) tuple for output.

    item is (user_id, (window_id, events_list)) from the KeyedStream.
    """
    user_id, (window_id, events) = item
    event_types = [e["event_type"] for e in events]
    result = {"user_id": user_id, "events": event_types}
    return (user_id, json.dumps(result))


def build_flow() -> Dataflow:
    """Build the session windowing dataflow."""
    flow = Dataflow("session_windowing")

    # Read input lines from input.jsonl
    lines = inp("read_input", flow, FileSource(Path("input.jsonl")))

    # Parse JSON lines
    events = map("parse_json", lines, parse_line)

    # Key events by user_id
    keyed_events = map("key_on_user", events, key_on_user)

    # Apply session windowing with 5-second gap
    clock = EventClock(
        ts_getter=ts_getter,
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = SessionWindower(gap=timedelta(seconds=5))

    windowed = collect_window(
        "session_window",
        keyed_events,
        clock=clock,
        windower=windower,
    )

    # Format output as (key, json_string) tuples
    formatted = map("format_output", windowed.down, format_output)

    # Write to output.jsonl
    output("write_output", formatted, FileSink(Path("output.jsonl")))

    return flow


# bytewax.run looks for a top-level `flow` attribute
flow = build_flow()
