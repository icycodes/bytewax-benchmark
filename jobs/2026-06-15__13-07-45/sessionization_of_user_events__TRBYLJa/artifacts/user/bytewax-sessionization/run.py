import json
from datetime import datetime, timedelta, timezone

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from bytewax.operators.windowing import (
    EventClock,
    SessionWindower,
    collect_window,
)
from bytewax.run import cli_main


class JSONLinesSource(FixedPartitionedSource):
    """A source that reads a JSON lines file from a single partition."""

    def __init__(self, path):
        self.path = path

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id, for_part, resume_state):
        return _JSONLinesPartition(self.path)


class _JSONLinesPartition(StatefulSourcePartition):
    def __init__(self, path):
        self._path = path
        self._lines = None

    def next_batch(self):
        if self._lines is None:
            with open(self._path, "r") as f:
                self._lines = f.readlines()
        if self._lines:
            batch = self._lines
            self._lines = []
            return [json.loads(line.strip()) for line in batch]
        raise StopIteration

    def snapshot(self):
        return None


class JSONLinesSink(FixedPartitionedSink):
    """A sink that writes JSON objects to a file, one per line."""

    def __init__(self, path):
        self.path = path

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id, for_part, resume_state):
        return _JSONLinesSinkPartition(self.path)


class _JSONLinesSinkPartition(StatefulSinkPartition):
    def __init__(self, path):
        self.path = path
        self._file = None

    def write_batch(self, items):
        if self._file is None:
            self._file = open(self.path, "w")
        for item in items:
            self._file.write(json.dumps(item) + "\n")

    def snapshot(self):
        return None

    def close(self):
        if self._file is not None:
            self._file.close()


def parse_ts(event):
    """Extract datetime from event timestamp."""
    return datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))


def build_session_output(item):
    """Build the session summary from collected events.

    item is (user_id, (window_id, events)) from collect_window's down stream.
    """
    user_id, (window_id, events) = item
    # Sort events by timestamp to get correct start/end
    events_sorted = sorted(events, key=lambda e: parse_ts(e))
    return {
        "user_id": user_id,
        "session_start": events_sorted[0]["timestamp"],
        "session_end": events_sorted[-1]["timestamp"],
        "event_count": len(events),
    }


def build_flow():
    flow = Dataflow("sessionization")

    # Read input
    inp = op.input("read_input", flow, JSONLinesSource("user_events.jsonl"))

    # Key by user_id: Stream[Tuple[str, dict]]
    keyed = op.key_on("key_on_user", inp, lambda e: e["user_id"])

    # Define clock and windower
    clock = EventClock(
        ts_getter=lambda e: parse_ts(e),
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = SessionWindower(gap=timedelta(minutes=30))

    # Collect events into session windows
    window_out = collect_window(
        "sessionize",
        keyed,
        clock=clock,
        windower=windower,
    )

    # window_out.down is KeyedStream[Tuple[int, List[dict]]]
    # Keyed by (user_id, window_id)
    # We need to pair with metadata to get open/close times
    # But actually, collect_window gives us the events list directly.
    # Let's just map the down stream.

    # Map to session summaries: (user_id, (window_id, events)) -> dict
    sessions = op.map(
        "build_sessions",
        window_out.down,
        lambda item: build_session_output(item),
    )

    # Key by user_id for the sink (output requires keyed streams)
    sessions_keyed = op.key_on(
        "key_sessions",
        sessions,
        lambda s: s["user_id"],
    )

    # Write output
    op.output("write_output", sessions_keyed, JSONLinesSink("sessions.jsonl"))

    return flow


if __name__ == "__main__":
    flow = build_flow()
    cli_main(flow)
