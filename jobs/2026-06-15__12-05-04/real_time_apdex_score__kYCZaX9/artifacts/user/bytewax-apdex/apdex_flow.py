"""
Real-Time Apdex Score Calculation with Bytewax
===============================================

Reads JSON-line events from input.jsonl, computes the Apdex score per
service using 10-second tumbling event-time windows, and writes results
to output.jsonl.

Apdex threshold T = 500 ms
  Satisfied  : response_time_ms <= 500
  Tolerating : 500 < response_time_ms <= 2000
  Frustrated : response_time_ms > 2000

Formula: (satisfied + tolerating / 2) / total
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    fold_window,
)
from bytewax.testing import run_main

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APDEX_T_MS = 500          # Apdex threshold in milliseconds
TOLERATE_MAX_MS = 2000     # Upper bound for tolerating zone
WINDOW_LENGTH = timedelta(seconds=10)
# Align windows to the Unix epoch (UTC midnight 1970-01-01) so that window
# boundaries are always multiples of 10 seconds from that reference point.
ALIGN_TO = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Wait 0 s for late events because input is a static file read in timestamp
# order.  Increase this when ingesting out-of-order live streams.
WAIT_FOR_SYSTEM_DURATION = timedelta(seconds=0)

# ---------------------------------------------------------------------------
# Accumulator
# ---------------------------------------------------------------------------

@dataclass
class ApdexAccumulator:
    """Counts of each Apdex bucket within a single window / service key."""
    satisfied: int = 0
    tolerating: int = 0
    frustrated: int = 0

    @property
    def total(self) -> int:
        return self.satisfied + self.tolerating + self.frustrated

    @property
    def score(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.satisfied + self.tolerating / 2.0) / self.total

    def merge(self, other: "ApdexAccumulator") -> "ApdexAccumulator":
        return ApdexAccumulator(
            satisfied=self.satisfied + other.satisfied,
            tolerating=self.tolerating + other.tolerating,
            frustrated=self.frustrated + other.frustrated,
        )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_event(line: str):
    """
    Parse a raw JSON line into a ``(service, event_dict)`` keyed pair.

    Returns ``None`` for blank / malformed lines so they can be filtered out.
    """
    line = line.strip()
    if not line:
        return None
    try:
        event = json.loads(line)
        return (event["service"], event)
    except (json.JSONDecodeError, KeyError):
        return None


def get_event_timestamp(event: dict) -> datetime:
    """Extract the event timestamp as an aware UTC datetime."""
    ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    # Ensure the datetime carries UTC timezone info (EventClock requirement).
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def build_accumulator() -> ApdexAccumulator:
    """Factory for an empty accumulator (called once per new window+key)."""
    return ApdexAccumulator()


def fold_event(acc: ApdexAccumulator, event: dict) -> ApdexAccumulator:
    """Classify the event and increment the appropriate counter."""
    rt = event["response_time_ms"]
    if rt <= APDEX_T_MS:
        acc.satisfied += 1
    elif rt <= TOLERATE_MAX_MS:
        acc.tolerating += 1
    else:
        acc.frustrated += 1
    return acc


def merge_accumulators(
    a: ApdexAccumulator, b: ApdexAccumulator
) -> ApdexAccumulator:
    """Merge two accumulators (required by fold_window for window merges)."""
    return a.merge(b)


def format_result(keyed_window_output) -> tuple:
    """
    Convert ``(service, (window_id: int, ApdexAccumulator))`` into a
    ``(key, json_line)`` tuple for FileSink.

    ``windowed.down`` emits ``(key, (window_id, accumulator))`` where
    ``window_id`` is the integer index of the window from ``ALIGN_TO``.
    The window open time is therefore: ``ALIGN_TO + window_id * WINDOW_LENGTH``.

    FileSink expects ``(key, value)`` pairs where ``value`` is a string.
    """
    service, (window_id, acc) = keyed_window_output
    open_time = ALIGN_TO + window_id * WINDOW_LENGTH
    window_start = open_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    result = {
        "window_start": window_start,
        "service": service,
        "apdex_score": round(acc.score, 2),
    }
    return (service, json.dumps(result))


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

INPUT_PATH = Path(__file__).parent / "input.jsonl"
OUTPUT_PATH = Path(__file__).parent / "output.jsonl"

# Truncate the output file before running so stale results don't accumulate.
OUTPUT_PATH.write_text("")

flow = Dataflow("apdex_flow")

# 1. Read raw lines from the input file
raw = op.input("read_input", flow, FileSource(INPUT_PATH))

# 2. Parse each line → (service, event_dict); drop None (blank/bad lines)
parsed = op.filter_map("parse_event", raw, parse_event)

# 3. Configure the event-time clock using the "timestamp" field
clock = EventClock(
    ts_getter=get_event_timestamp,
    wait_for_system_duration=WAIT_FOR_SYSTEM_DURATION,
)

# 4. Configure a 10-second tumbling window aligned to the epoch
windower = TumblingWindower(length=WINDOW_LENGTH, align_to=ALIGN_TO)

# 5. Aggregate (fold) events into ApdexAccumulators per (service, window)
windowed = fold_window(
    "fold_apdex",
    parsed,
    clock,
    windower,
    builder=build_accumulator,
    folder=fold_event,
    merger=merge_accumulators,
)

# windowed.down emits (service, (window_id: int, ApdexAccumulator)) tuples
# windowed.late  emits items that arrived after the watermark (dropped here)

# 6. Serialize each result to a JSON string
output_stream = op.map("format_result", windowed.down, format_result)

# 7. Write JSON lines to the output file
op.output("write_output", output_stream, FileSink(OUTPUT_PATH))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_main(flow)
