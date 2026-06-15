"""Bytewax dataflow for sliding-window outlier detection on sensor temperatures.

Reads JSON lines from input.jsonl, computes sliding-window mean and
population standard deviation per sensor, and writes results to output.jsonl.
"""

import json
import math
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow

ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_LENGTH = timedelta(seconds=60)
WINDOW_OFFSET = timedelta(seconds=30)


def parse_line(line: str) -> dict:
    """Parse a JSON line into a dict."""
    return json.loads(line)


def extract_sensor_id(record: dict) -> str:
    """Extract the sensor_id as the key (must be a string)."""
    return record["sensor_id"]


def extract_timestamp(record: dict) -> datetime:
    """Extract the event time as a timezone-aware UTC datetime."""
    # Parse ISO8601 string and ensure it's UTC-aware
    dt = datetime.fromisoformat(record["time"].replace("Z", "+00:00"))
    return dt


# ---------------------------------------------------------------------------
# fold_window callbacks: builder, folder, merger
# All use pure Python dicts (fully picklable) as accumulators.
# ---------------------------------------------------------------------------


def builder() -> dict:
    """Return an empty accumulator for a new window.

    Accumulator is a plain dict with:
      - sum: running sum of temperatures
      - sum_sq: running sum of squared temperatures
      - count: number of values seen
    """
    return {"sum": 0.0, "sum_sq": 0.0, "count": 0}


def folder(acc: dict, record: dict) -> dict:
    """Fold a temperature value from a record into the accumulator."""
    temp = record["temp"]
    acc["sum"] += temp
    acc["sum_sq"] += temp * temp
    acc["count"] += 1
    return acc


def merger(a: dict, b: dict) -> dict:
    """Merge two accumulators (required by API; used by SessionWindower).

    For SlidingWindower this is typically not called, but must be
    defined for the API and must be picklable.
    """
    return {
        "sum": a["sum"] + b["sum"],
        "sum_sq": a["sum_sq"] + b["sum_sq"],
        "count": a["count"] + b["count"],
    }


# ---------------------------------------------------------------------------
# Window output processing
# ---------------------------------------------------------------------------


def compute_stats(item) -> dict:
    """Compute mean and population stddev from a window accumulator.

    The input item is (sensor_id, (window_id, accumulator_dict)).
    We also have the window metadata from the join step.
    """
    (sensor_id, window_meta), acc = item

    count = acc["count"]
    if count == 0:
        mean = 0.0
        stddev = 0.0
    else:
        mean = acc["sum"] / count
        # population stddev = sqrt(E[X^2] - E[X]^2)
        variance = (acc["sum_sq"] / count) - (mean * mean)
        # Guard against floating-point imprecision giving tiny negative values
        if variance < 0:
            variance = 0.0
        stddev = math.sqrt(variance)

    return {
        "sensor_id": sensor_id,
        "window_start": window_meta.open_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": window_meta.close_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mean": mean,
        "stddev": stddev,
    }


def format_output(record: dict) -> str:
    """Format a result dict as a JSON line."""
    return json.dumps(record)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("sensor_sliding_window")

# 1. Read input
lines = op.input("input", flow, FileSource("input.jsonl"))

# 2. Parse JSON lines
records = op.map("parse_json", lines, parse_line)

# 3. Key by sensor_id (must be string for stateful operators)
keyed = op.key_on("key_on_sensor", records, extract_sensor_id)

# 4. Define event-time clock
clock = win.EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# 6. Define sliding windower
windower = win.SlidingWindower(
    length=WINDOW_LENGTH,
    offset=WINDOW_OFFSET,
    align_to=ALIGN_TO,
)

# 7. fold_window: accumulate temperatures per sensor per window
windowed = win.fold_window(
    "fold_window",
    keyed,  # use the original keyed stream (with full records) for clock
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger,
)

# 8. Join the window accumulators (.down) with window metadata (.meta)
#    .down emits: (sensor_id, (window_id, accumulator))
#    .meta emits: (sensor_id, (window_id, WindowMetadata))
joined = op.join("join_meta", windowed.down, windowed.meta)

# 9. Compute mean and stddev, format as output dict
results = op.map("compute_stats", joined, compute_stats)

# 10. Format as JSON lines
formatted = op.map("format_json", results, format_output)

# 11. Write to output file
op.output("output", formatted, FileSink("output.jsonl"))
