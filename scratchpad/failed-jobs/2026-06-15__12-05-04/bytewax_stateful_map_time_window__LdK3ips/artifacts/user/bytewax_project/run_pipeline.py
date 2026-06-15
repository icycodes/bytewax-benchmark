"""Bytewax v0.21.1 – stateful event deduplication pipeline.

Reads JSON-lines from events.json, deduplicates events per user within
a 10-second sliding window (based on event timestamps), and writes the
surviving events to output.json.

Deduplication logic
-------------------
* State per user: ``Dict[event_id, datetime]`` – the first-seen timestamp
  for every event_id that is still within the 10-second window.
* On each event:
  1. Prune state: remove entries whose recorded timestamp is more than
     10 seconds older than the *current* event's timestamp.
  2. If the event_id is now absent from state → unique; record it and
     emit the event.
  3. If the event_id is still present → duplicate; emit ``None``.
* A downstream ``filter_map`` drops ``None`` values (duplicates).
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEDUP_WINDOW_SECONDS = 10

# State type alias: event_id -> first-seen datetime (timezone-aware)
SeenState = Dict[str, datetime]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO 8601 string into a timezone-aware ``datetime``."""
    # Python ≤3.10 does not accept the 'Z' suffix in fromisoformat.
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Stateful mapper
# ---------------------------------------------------------------------------

def dedup_mapper(
    state: Optional[SeenState],
    event_json: str,
) -> Tuple[Optional[SeenState], Optional[str]]:
    """Deduplicate one event within a per-user 10-second window.

    Called by ``op.stateful_map`` for every ``(user_id, event_json)``
    item.  Returns ``(new_state, output)`` where ``output`` is the
    original JSON string for a unique event or ``None`` for a duplicate.

    Args:
        state:      Current per-user state, or ``None`` on first event.
        event_json: Raw JSON string of the event (the *value* part of
                    the keyed stream tuple).

    Returns:
        A ``(new_state, output)`` 2-tuple.
    """
    if state is None:
        state = {}

    event = json.loads(event_json)
    event_id: str = event["event_id"]
    current_ts: datetime = _parse_ts(event["timestamp"])

    # 1. Prune stale entries (older than DEDUP_WINDOW_SECONDS relative to now).
    state = {
        eid: ts
        for eid, ts in state.items()
        if (current_ts - ts).total_seconds() <= DEDUP_WINDOW_SECONDS
    }

    # 2. Duplicate check.
    if event_id in state:
        # Seen within the window – emit nothing but keep state.
        return (state, None)

    # 3. New event – record and pass through.
    state[event_id] = current_ts
    return (state, event_json)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("event_dedup")

# --- Input: read events.json line-by-line ----------------------------------
# FileSource yields one stripped string per line.
raw: op.Stream = op.input(
    "read_events",
    flow,
    FileSource("events.json"),
)

# --- Key by user_id (Bytewax requires string keys) -------------------------
# Produces Stream[Tuple[str, str]]  →  (user_id, event_json)
keyed: op.Stream = op.key_on(
    "key_by_user",
    raw,
    lambda line: json.loads(line)["user_id"],
)

# --- Stateful deduplication ------------------------------------------------
# stateful_map emits (user_id, Optional[str]); None means duplicate.
deduped: op.Stream = op.stateful_map(
    "dedup",
    keyed,
    dedup_mapper,
)

# --- Drop duplicates -------------------------------------------------------
# filter_map receives the full (key, value) tuple from the keyed stream and
# must return either a non-None value (passed downstream) or None (dropped).
# FileSink is a FixedPartitionedSink[str] which expects (key, value) tuples,
# so we preserve the (user_id, event_json) structure and only drop rows where
# the inner value is None.
unique: op.Stream = op.filter_map(
    "drop_dupes",
    deduped,
    # kv == (user_id, Optional[event_json])
    # Return the tuple unchanged when event_json is not None; else None.
    lambda kv: kv if kv[1] is not None else None,
)

# --- Output: write deduplicated events to output.json ----------------------
# FileSink[str] expects (key, value) where value is the string to write.
op.output(
    "write_output",
    unique,
    FileSink(Path("output.json")),
)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from bytewax.testing import run_main

    run_main(flow)
