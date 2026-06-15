"""
Bytewax stateful deduplication dataflow.

Reads JSON events from input.jsonl, deduplicates them per user_id over a
10-second sliding window (based on event timestamps), and writes the
surviving events to output.jsonl.

Deduplication rule:
  - An event is DROPPED  if the same event_id was seen within the last 10 s
    (i.e. current_ts <= previous_ts + 10 s).
  - An event is EMITTED  if the event_id has never been seen, or was last
    seen strictly more than 10 s ago (current_ts > previous_ts + 10 s).
    In that case the 10-second window restarts for that event_id.

State: dict[event_id -> datetime of last emitted occurrence]
State cleanup: event_ids older than 10 s from the current event timestamp
are pruned on every call to keep memory bounded.
"""

import json
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic
from bytewax.testing import run_main

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WINDOW_SECONDS = 10
INPUT_PATH = Path(__file__).parent / "input.jsonl"
OUTPUT_PATH = Path(__file__).parent / "output.jsonl"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware datetime."""
    # Python 3.11+ handles 'Z' directly; for older versions replace manually.
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def _parse_line(line: str) -> Tuple[str, dict]:
    """Parse a JSON line and return ``(user_id, event_dict)``."""
    event = json.loads(line)
    event["_ts"] = _parse_ts(event["timestamp"])  # attach parsed timestamp
    return event["user_id"], event


# ---------------------------------------------------------------------------
# Stateful deduplication logic
# ---------------------------------------------------------------------------

# State type: Dict[event_id: str, last_seen: datetime]
DedupeState = Dict[str, datetime]


class DeduplicateLogic(StatefulLogic[dict, dict, DedupeState]):
    """Per-user_id stateful deduplication over a 10-second event-time window.

    State:
        A dict mapping ``event_id`` → ``datetime`` of the last *emitted*
        occurrence for that event_id under this user_id.
    """

    def __init__(self, state: Optional[DedupeState]) -> None:
        # Restore from snapshot or start fresh.
        self._seen: DedupeState = state if state is not None else {}

    def on_item(self, event: dict) -> Tuple[Iterable[dict], bool]:
        current_ts: datetime = event["_ts"]
        event_id: str = event["event_id"]
        window = timedelta(seconds=WINDOW_SECONDS)

        # --- State cleanup: remove event_ids older than 10 s from now -------
        expired = [
            eid
            for eid, last_ts in self._seen.items()
            if current_ts > last_ts + window
        ]
        for eid in expired:
            del self._seen[eid]

        # --- Deduplication check --------------------------------------------
        if event_id in self._seen:
            last_ts = self._seen[event_id]
            if current_ts <= last_ts + window:
                # Duplicate within the window → drop.
                return [], StatefulLogic.RETAIN

        # New or expired event_id → emit and record timestamp.
        self._seen[event_id] = current_ts
        return [event], StatefulLogic.RETAIN

    def on_notify(self) -> Tuple[Iterable[dict], bool]:
        return [], StatefulLogic.RETAIN

    def on_eof(self) -> Tuple[Iterable[dict], bool]:
        return [], StatefulLogic.RETAIN

    def notify_at(self) -> Optional[datetime]:
        return None  # no timer-based notifications needed

    def snapshot(self) -> DedupeState:
        # Return an immutable (deep) copy so the runtime can pickle it safely.
        return copy.deepcopy(self._seen)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _format_event(keyed: Tuple[str, dict]) -> Tuple[str, str]:
    """Serialize the event back to a keyed JSON string (strip internal _ts field).

    FileSink is a FixedPartitionedSink that expects ``(key, value)`` tuples
    where *value* must be a ``str``.  We keep the key so routing works and
    pass the JSON-serialised event as the value.
    """
    key, event = keyed
    output = {k: v for k, v in event.items() if k != "_ts"}
    return key, json.dumps(output)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("deduplication")

# 1. Read raw lines from the input file.
raw: op.Stream[str] = op.input("read", flow, FileSource(INPUT_PATH))

# 2. Parse each line into a keyed (user_id, event_dict) tuple.
keyed: op.Stream[Tuple[str, dict]] = op.map("parse", raw, _parse_line)

# 3. Apply stateful deduplication per user_id.
deduped: op.Stream[Tuple[str, dict]] = op.stateful(
    "deduplicate",
    keyed,
    lambda state: DeduplicateLogic(state),
)

# 4. Format surviving events back to keyed JSON strings: (user_id, json_str).
formatted: op.Stream[Tuple[str, str]] = op.map("format", deduped, _format_event)

# 5. Write to output file.
op.output("write", formatted, FileSink(OUTPUT_PATH))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ensure a clean output file before each run.
    OUTPUT_PATH.unlink(missing_ok=True)
    OUTPUT_PATH.touch()
    run_main(flow)
    print(f"Done. Results written to {OUTPUT_PATH}")
