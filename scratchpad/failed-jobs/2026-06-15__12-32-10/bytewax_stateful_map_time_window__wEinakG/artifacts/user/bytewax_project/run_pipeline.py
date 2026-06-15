import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

DEDUP_WINDOW_SECS = 10


def parse_event(line):
    """Parse a JSON line into an event dict."""
    return json.loads(line)


def deduplicate(state, event):
    """Stateful mapper for event deduplication.

    State is a dict mapping event_id -> datetime (timestamp of first
    occurrence within the dedup window).

    For each incoming event:
    1. Prune old entries from state (older than DEDUP_WINDOW_SECS
       relative to the current event's timestamp).
    2. Check if event_id exists in the pruned state.
    3. If duplicate (event_id found within window), emit None.
    4. If not duplicate, add event_id to state and emit the event.

    Returns (updated_state, event_or_None).
    """
    if state is None:
        state = {}

    event_ts = datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )
    event_id = event["event_id"]
    window = timedelta(seconds=DEDUP_WINDOW_SECS)

    # Prune old entries from state
    pruned = {
        eid: ts
        for eid, ts in state.items()
        if event_ts - ts <= window
    }

    # Check for duplicate
    if event_id in pruned:
        return (pruned, None)

    # Not a duplicate; add to state and emit
    pruned[event_id] = event_ts
    return (pruned, event)


def format_event(event):
    """Format an event dict as a JSON string."""
    return json.dumps(event)


flow = Dataflow("dedup")

# Read input events line by line
inp = op.input("read_input", flow, FileSource("events.json"))

# Parse each line as JSON
parsed = op.map("parse_json", inp, parse_event)

# Key the stream by user_id
keyed = op.key_on("key_by_user", parsed, lambda e: e["user_id"])

# Stateful deduplication per user
deduped = op.stateful_map("dedup", keyed, deduplicate)

# Filter out None values (duplicates)
filtered = op.filter_value("filter_none", deduped, lambda v: v is not None)

# Format each event value as a JSON string (keeping the key for sink routing)
output = op.map_value("format_json", filtered, format_event)

# Write deduplicated events to output file
op.output("write_output", output, FileSink(Path("output.json")))

if __name__ == "__main__":
    run_main(flow)