"""Session-based clickstream aggregator using Bytewax.

Groups user click events into dynamic session windows based on 10-second
inactivity gaps and computes total pages visited per session.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, SessionWindower, fold_window


# ---------------------------------------------------------------------------
# Custom JSONL file sink (DynamicSink accepts any item type, no key required)
# ---------------------------------------------------------------------------

class _JsonlPartition(StatelessSinkPartition[str]):
    """Writes string items line-by-line to a file."""

    def __init__(self, path: Path) -> None:
        self._fh = path.open("a", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._fh.write(item + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class JsonlSink(DynamicSink[str]):
    """Append-mode JSONL file sink compatible with non-keyed streams."""

    def __init__(self, path: Path) -> None:
        self._path = path
        # Truncate on startup so each run produces a clean file.
        path.open("w").close()

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _JsonlPartition:
        return _JsonlPartition(self._path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_event(raw_line: str):
    """Parse a JSON line into a (user_id, event_dict) keyed tuple."""
    event = json.loads(raw_line)
    return (event["user_id"], event)


def get_event_timestamp(event: dict) -> datetime:
    """Extract the event timestamp as a timezone-aware UTC datetime."""
    ts = event["timestamp"]
    # Normalise the 'Z' suffix for Python < 3.11 compatibility.
    ts = ts.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    return dt.astimezone(timezone.utc)


def format_output(user_id_and_window_result: tuple) -> str:
    """Map (user_id, (window_id, total_pages)) → JSON string."""
    user_id, (_, total_pages) = user_id_and_window_result
    return json.dumps({"user_id": user_id, "total_pages": total_pages})


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("clickstream_sessions")

# 1. Read raw lines from input.jsonl
raw = op.input("input", flow, FileSource(Path("input.jsonl")))

# 2. Parse each line and key the stream by user_id
keyed = op.map("parse", raw, parse_event)

# 3. Event-time clock: timestamps come from within each event.
#    wait_for_system_duration=0 is appropriate for batch/ordered file input.
clock = EventClock(
    ts_getter=get_event_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# 4. Session window: a session closes after 10 seconds of inactivity per user.
windower = SessionWindower(gap=timedelta(seconds=10))

# 5. Fold events within each session window – count the number of page visits.
window_out = fold_window(
    "session_window",
    keyed,
    clock,
    windower,
    builder=lambda: 0,                       # initial accumulator per session
    folder=lambda acc, _event: acc + 1,      # +1 for every event
    merger=lambda a, b: a + b,               # merge counts across workers
)

# 6. window_out.down yields (user_id, (window_id, total_pages)) tuples.
#    Map to compact JSON strings for output.
formatted = op.map("format", window_out.down, format_output)

# 7. Write one JSON object per line to output.jsonl via our custom sink.
op.output("output", formatted, JsonlSink(Path("output.jsonl")))
