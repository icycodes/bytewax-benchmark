"""
Bytewax stateful deduplication dataflow.

Reads JSON events from input.jsonl, deduplicates them per user_id so that
the same event_id is only emitted once per 10-second window (based on
event timestamps), and writes the results to output.jsonl.

Deduplication rule:
  - Emit the event and record its timestamp the first time an event_id is seen
    for a given user_id.
  - Drop the event if it arrives within 10 seconds of the previously emitted
    occurrence (i.e. event_timestamp <= last_seen_timestamp + 10s).
  - Emit again (and reset the window) if it arrives strictly after 10 seconds
    (i.e. event_timestamp > last_seen_timestamp + 10s).
  - Evict stale entries: any event_id whose last-seen timestamp is more than
    10 seconds in the past relative to the current event's timestamp is removed
    from state to prevent unbounded memory growth.
"""

import json
import copy
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic
from bytewax.testing import run_main

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEDUP_WINDOW = timedelta(seconds=10)

INPUT_PATH = Path(__file__).parent / "input.jsonl"
OUTPUT_PATH = Path(__file__).parent / "output.jsonl"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def parse_event(line: str) -> dict:
    """Parse a raw JSON line into an event dict with a parsed datetime."""
    event = json.loads(line)
    # Parse ISO 8601 timestamp; replace trailing 'Z' for Python < 3.11
    ts_str = event["timestamp"].replace("Z", "+00:00")
    event["_ts"] = datetime.fromisoformat(ts_str)
    return event


def key_event(event: dict) -> Tuple[str, dict]:
    """Key events by user_id for stateful processing."""
    return event["user_id"], event


def format_event(keyed: Tuple[str, dict]) -> Tuple[str, str]:
    """Serialize the event back to a JSON string (without the internal _ts key).

    Returns a (key, json_string) tuple so that FileSink can route by key.
    """
    key, event = keyed
    output = {k: v for k, v in event.items() if k != "_ts"}
    return key, json.dumps(output)


# ---------------------------------------------------------------------------
# Stateful deduplication logic
# ---------------------------------------------------------------------------


class DeduplicationLogic(StatefulLogic):
    """Per-user_id stateful deduplication logic.

    State: Dict[event_id, last_emitted_timestamp]
    """

    def __init__(self, state: Optional[Dict[str, datetime]]) -> None:
        # Map from event_id -> datetime of last emission
        self._seen: Dict[str, datetime] = state if state is not None else {}

    def on_item(
        self, event: dict
    ) -> Tuple[Iterable[dict], bool]:
        current_ts: datetime = event["_ts"]

        # --- Step 1: evict stale entries ---
        # Remove any event_id whose last-seen time is more than 10 seconds
        # before the current event's timestamp.
        stale = [
            eid
            for eid, last_ts in self._seen.items()
            if current_ts - last_ts > DEDUP_WINDOW
        ]
        for eid in stale:
            del self._seen[eid]

        # --- Step 2: deduplication check ---
        event_id = event["event_id"]
        emit: list[dict] = []

        if event_id not in self._seen:
            # First time we see this event_id: emit and record timestamp.
            self._seen[event_id] = current_ts
            emit.append(event)
        else:
            last_ts = self._seen[event_id]
            if current_ts > last_ts + DEDUP_WINDOW:
                # Strictly after the 10-second window: re-emit and reset clock.
                self._seen[event_id] = current_ts
                emit.append(event)
            # else: duplicate within window – drop silently.

        return emit, StatefulLogic.RETAIN

    def on_eof(self) -> Tuple[Iterable[dict], bool]:
        return [], StatefulLogic.DISCARD

    def notify_at(self) -> Optional[datetime]:
        return None

    def on_notify(self) -> Tuple[Iterable[dict], bool]:  # pragma: no cover
        return [], StatefulLogic.RETAIN

    def snapshot(self) -> Dict[str, datetime]:
        # Return a deep copy so the runtime's pickled snapshot is independent
        # of our live state dict.
        return copy.deepcopy(self._seen)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

# Truncate the output file before running so we start fresh.
OUTPUT_PATH.write_text("")

flow = Dataflow("dedup")

# 1. Read raw lines from input.jsonl
raw = op.input("read", flow, FileSource(INPUT_PATH))

# 2. Parse JSON lines into event dicts
events = op.map("parse", raw, parse_event)

# 3. Key by user_id  →  Stream[Tuple[str, dict]]
keyed = op.key_on("key_by_user", events, lambda e: e["user_id"])

# 4. Stateful deduplication (one DeduplicationLogic instance per user_id)
deduped = op.stateful(
    "dedup",
    keyed,
    lambda state: DeduplicationLogic(state),
)

# 5. Serialize back to JSON strings
json_out = op.map("serialize", deduped, format_event)

# 6. Write to output.jsonl
op.output("write", json_out, FileSink(OUTPUT_PATH))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_main(flow)
