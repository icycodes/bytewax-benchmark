import json
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Optional

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main


class EventsPartition(StatefulSourcePartition):
    """A single partition that reads events from events.json."""

    def __init__(self):
        self._events = []
        self._idx = 0
        with open("events.json") as f:
            for line in f:
                line = line.strip()
                if line:
                    self._events.append(json.loads(line))

    def next_batch(self) -> Iterable[dict]:
        if self._idx < len(self._events):
            batch = [self._events[self._idx]]
            self._idx += 1
            return batch
        raise StopIteration

    def snapshot(self):
        return self._idx


class EventsSource(FixedPartitionedSource):
    def list_parts(self):
        return ["single"]

    def build_part(self, step_id, for_part, resume_state):
        return EventsPartition()


class FileSinkPartition(StatelessSinkPartition):
    def __init__(self, path):
        self._path = path
        # Clear the file at start
        with open(self._path, "w") as f:
            pass

    def write_batch(self, items: List[str]) -> None:
        with open(self._path, "a") as f:
            for item in items:
                f.write(item + "\n")


class FileSink(DynamicSink):
    def __init__(self, path):
        self._path = path

    def build(self, step_id, worker_index, worker_count):
        return FileSinkPartition(self._path)


def key_on_user(event):
    """Key by user_id (must be a string for Bytewax)."""
    return str(event["user_id"])


def stateful_dedup(state, event):
    """
    Maintain a dict of {event_id: timestamp} per user key.
    Returns None if duplicate (event_id seen within 10 seconds),
    otherwise returns the event and updates state.
    Also prunes old entries from state.

    In Bytewax 0.21 stateful_map, `state` is None on first call
    for a given key.
    """
    if state is None:
        state = {}

    event_id = event["event_id"]
    event_ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    window = timedelta(seconds=10)

    # Prune old entries from state (entries older than 10s relative to current event)
    stale_ids = [eid for eid, ts in state.items() if event_ts - ts > window]
    for eid in stale_ids:
        del state[eid]

    # Check if this event_id is a duplicate
    if event_id in state:
        prev_ts = state[event_id]
        if event_ts - prev_ts <= window:
            # Duplicate within 10-second window
            return state, None

    # Not a duplicate — record it and emit
    state[event_id] = event_ts
    return state, event


def serialize(event):
    """Serialize event to JSON line."""
    return json.dumps(event)


flow = Dataflow("event_dedup")

# Input: read events from events.json
input_stream = op.input("input", flow, EventsSource())

# Key by user_id
keyed = op.key_on("key_on_user", input_stream, key_on_user)

# Stateful deduplication
deduped = op.stateful_map("dedup", keyed, stateful_dedup)

# Filter out None values (duplicates).
# deduped is a keyed stream of (key, value_or_None), so we check x[1] is not None.
filtered = op.filter("filter_none", deduped, lambda x: x[1] is not None)

# Extract just the event (drop the key tuple)
events_only = op.map("unwrap", filtered, lambda x: x[1])

# Serialize to JSON
serialized = op.map("serialize", events_only, serialize)

# Output to output.json
op.output("output", serialized, FileSink("output.json"))

# Run the dataflow
run_main(flow)
