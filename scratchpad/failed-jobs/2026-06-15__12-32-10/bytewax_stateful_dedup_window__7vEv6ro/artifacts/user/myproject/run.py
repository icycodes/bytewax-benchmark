import json
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op
from bytewax.operators import StatefulLogic
from bytewax.testing import run_main

INPUT_PATH = "/home/user/myproject/input.jsonl"
OUTPUT_PATH = "/home/user/myproject/output.jsonl"

DEDUP_WINDOW = timedelta(seconds=10)


class DedupLogic(StatefulLogic):
    """Stateful deduplication logic that tracks event_id -> last_emitted_timestamp per user_id."""

    def __init__(self, resume_state):
        # resume_state is a dict mapping event_id -> ISO timestamp string of last emission
        if resume_state is not None:
            self.seen = {
                eid: datetime.fromisoformat(ts) for eid, ts in resume_state.items()
            }
        else:
            self.seen = {}

    def on_item(self, event):
        event_id = event["event_id"]
        event_ts = event["timestamp"]

        # Clean up expired entries: remove event_ids older than 10 seconds from current event's timestamp
        expired = [
            eid
            for eid, ts in self.seen.items()
            if event_ts - ts > DEDUP_WINDOW
        ]
        for eid in expired:
            del self.seen[eid]

        # Check if this event_id was seen within the 10-second window
        if event_id in self.seen:
            last_ts = self.seen[event_id]
            # If within 10 seconds (inclusive), drop the event
            if event_ts <= last_ts + DEDUP_WINDOW:
                return [], self.RETAIN

        # Emit the event and update state
        self.seen[event_id] = event_ts
        return [event], self.RETAIN

    def snapshot(self):
        # Return immutable copy of state as dict of event_id -> ISO timestamp string
        return {eid: ts.isoformat() for eid, ts in self.seen.items()}


def parse_event(line):
    """Parse a JSON line into an event dict with a parsed timestamp."""
    data = json.loads(line)
    data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return data


def format_timestamp(dt):
    """Format datetime to ISO 8601 with Z suffix, removing trailing zeros from fractional seconds."""
    s = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        s += f".{frac}"
    return s + "Z"


def format_event(event):
    """Format an event dict into a JSON string for output."""
    out = dict(event)
    out["timestamp"] = format_timestamp(out["timestamp"])
    return json.dumps(out)


def main():
    flow = Dataflow("dedup")

    # Read input lines from file
    inp = op.input("read", flow, FileSource(INPUT_PATH))

    # Parse JSON strings into event dicts
    events = op.map("parse", inp, parse_event)

    # Key by user_id for stateful processing
    keyed = op.key_on("key_on_user", events, lambda e: e["user_id"])

    # Apply stateful deduplication
    deduped = op.stateful("dedup", keyed, DedupLogic)

    # Format event dicts to JSON strings (keeps keyed stream for FileSink routing)
    formatted = op.map_value("format", deduped, format_event)

    # Write to output file
    op.output("write", formatted, FileSink(OUTPUT_PATH))

    run_main(flow)


if __name__ == "__main__":
    main()