import json
from datetime import datetime, timedelta
from pathlib import Path

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic, stateful
import bytewax.operators as op
from bytewax.testing import run_main

INPUT_PATH = "/home/user/myproject/input.jsonl"
OUTPUT_PATH = "/home/user/myproject/output.jsonl"

DEDUP_WINDOW = timedelta(seconds=10)


def parse_event(line):
    """Parse a JSON line into an event dict with a datetime timestamp."""
    event = json.loads(line)
    event["timestamp"] = datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )
    return event


def format_event(event_dict):
    """Format an event dict back to a JSON string."""
    d = dict(event_dict)
    d["timestamp"] = d["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return json.dumps(d)


class DedupLogic(StatefulLogic):
    """Stateful deduplication logic within a 10-second sliding window.

    State maps event_id to the timestamp of its last emitted occurrence.
    An event is emitted only if its event_id has not been seen for the
    same user_id within the last 10 seconds. Entries older than 10 seconds
    from the current event's timestamp are cleaned up to prevent memory leaks.
    """

    def __init__(self, state=None):
        self.seen = state if state is not None else {}

    def on_item(self, value):
        event_id = value["event_id"]
        event_ts = value["timestamp"]

        # Clean up: remove event_ids older than 10 seconds from current timestamp
        cutoff = event_ts - DEDUP_WINDOW
        self.seen = {eid: ts for eid, ts in self.seen.items() if ts >= cutoff}

        # Check for duplicate within the 10-second window
        if event_id in self.seen:
            # Duplicate found — drop it
            return ([], False)

        # Not a duplicate — emit and record timestamp
        self.seen[event_id] = event_ts
        return ([value], False)

    def on_notify(self):
        return ([], False)

    def on_eof(self):
        return ([], True)

    def notify_at(self):
        return None

    def snapshot(self):
        return dict(self.seen)


def dedup_builder(state):
    return DedupLogic(state)


def build_flow():
    flow = Dataflow("dedup")

    # Read input lines from file
    inp = op.input("read", flow, FileSource(INPUT_PATH))

    # Parse each JSON line into a dict with a datetime timestamp
    parsed = op.map("parse", inp, parse_event)

    # Key the stream by user_id for stateful processing
    keyed = op.key_on("key_user", parsed, lambda e: e["user_id"])

    # Apply stateful deduplication per user_id
    deduped = stateful("dedup", keyed, dedup_builder)

    # Remove the key to get a stream of event dicts
    values = op.key_rm("unkey", deduped)

    # Format each event dict back to a JSON string
    output = op.map("format", values, format_event)

    # Write output lines to file
    op.output("write", output, FileSink(Path(OUTPUT_PATH)))

    return flow


if __name__ == "__main__":
    # Ensure output file exists (FileSink requires it)
    Path(OUTPUT_PATH).touch()
    run_main(build_flow())