"""Bytewax v0.21.1 sliding-window outlier detector with SQLite recovery support.

Run with:
    python -m bytewax.run pipeline:flow -r ./recovery_dir -s 1 -b 0
"""

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, SlidingWindower, WindowMetadata

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_LENGTH = timedelta(seconds=60)
WINDOW_OFFSET = timedelta(seconds=30)

# How long to wait for late events before advancing the watermark.
# For a bounded file source this can be short.
WAIT_FOR_SYSTEM = timedelta(seconds=10)

# ---------------------------------------------------------------------------
# Pure-Python accumulator  (must be picklable for SQLite recovery)
# ---------------------------------------------------------------------------


def _builder() -> list:
    """Return a fresh, empty accumulator for a window."""
    return []


def _folder(acc: list, record: dict) -> list:
    """Extract temperature from the record dict and append to the accumulator."""
    acc.append(record["temp"])
    return acc


def _merger(acc1: list, acc2: list) -> list:
    """Merge two partial accumulators (used during parallel reduction)."""
    return acc1 + acc2


# ---------------------------------------------------------------------------
# Helper: extract event timestamp from a parsed record
# ---------------------------------------------------------------------------


def _get_ts(record: dict) -> datetime:
    """Return a timezone-aware UTC datetime from the record's 'time' field."""
    raw = record["time"]
    # fromisoformat on Python 3.11+ handles the trailing 'Z'; use replace for
    # earlier versions as well.
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    # Ensure tz-aware UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("sliding_window_outlier_detector")

# 1. Read raw JSON lines from input file
raw_lines: op.Stream[str] = op.input(
    "read_input",
    flow,
    FileSource("input.jsonl"),
)

# 2. Parse each line into a dict
records: op.Stream[dict] = op.map(
    "parse_json",
    raw_lines,
    json.loads,
)

# 3. Key the stream by sensor_id (keys must be strings)
keyed: op.Stream[tuple] = op.key_on(
    "key_by_sensor",
    records,
    lambda r: r["sensor_id"],
)

# 4. Configure event-time clock and sliding windower
clock = EventClock(
    ts_getter=_get_ts,
    wait_for_system_duration=WAIT_FOR_SYSTEM,
)

windower = SlidingWindower(
    length=WINDOW_LENGTH,
    offset=WINDOW_OFFSET,
    align_to=ALIGN_TO,
)

# 5. Fold each window into a list of temperature values
window_out: win.WindowOut = win.fold_window(
    "fold_temps",
    keyed,
    clock,
    windower,
    _builder,
    _folder,
    _merger,
)

# 6. We need window metadata alongside the accumulated data to get timestamps.
#    Join the 'down' stream (accumulated values) with the 'meta' stream
#    (WindowMetadata containing open_time/close_time).
#
#    Strategy: key both by (sensor_id, window_id) then join.
#    In Bytewax 0.21 the WindowOut.down stream emits (sensor_id, (win_id, S))
#    and WindowOut.meta emits (sensor_id, (win_id, WindowMetadata)).
#    We re-key both to a compound string key and use join_window / collect, or
#    we handle it more simply by zipping meta into the downstream.
#
#    Simplest approach: map .meta to a lookup dict keyed the same way, then
#    join with .down using op.join on a per-(sensor,win_id) key.

def _rekey_down(item: tuple) -> tuple:
    """(sensor_id, (win_id, temps_list)) -> ((sensor_id, win_id), temps_list)"""
    sensor_id, (win_id, temps) = item
    return (f"{sensor_id}|{win_id}", temps)


def _rekey_meta(item: tuple) -> tuple:
    """(sensor_id, (win_id, WindowMetadata)) -> ((sensor_id, win_id), WindowMetadata)"""
    sensor_id, (win_id, meta) = item
    return (f"{sensor_id}|{win_id}", meta)


down_rekeyed: op.Stream[tuple] = op.map(
    "rekey_down",
    window_out.down,
    _rekey_down,
)

meta_rekeyed: op.Stream[tuple] = op.map(
    "rekey_meta",
    window_out.meta,
    _rekey_meta,
)

# op.join waits until both sides arrive for the same key then emits a tuple
joined = op.join("join_meta", down_rekeyed, meta_rekeyed)

# 7. Compute statistics and format output
def _compute_stats(item: tuple) -> tuple:
    """
    item: (compound_key, (temps_list, WindowMetadata))
    Returns a (key, json_string) tuple for the FileSink.
    """
    compound_key, (temps, meta) = item

    # Recover sensor_id from compound key
    sensor_id = compound_key.split("|", 1)[0]

    n = len(temps)
    if n == 0:
        mean = 0.0
        stddev = 0.0
    else:
        mean = sum(temps) / n
        variance = sum((t - mean) ** 2 for t in temps) / n  # population std-dev
        stddev = math.sqrt(variance)

    # Format timestamps as ISO 8601 UTC strings ending with 'Z'
    def _fmt(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result = {
        "sensor_id": sensor_id,
        "window_start": _fmt(meta.open_time),
        "window_end": _fmt(meta.close_time),
        "mean": round(mean, 6),
        "stddev": round(stddev, 6),
    }
    return (compound_key, json.dumps(result))


# KeyedStream[str] — FileSink routes by key (0th element) and writes the value
output_lines: op.Stream[tuple] = op.map(
    "compute_stats",
    joined,
    _compute_stats,
)

# 8. Write results to output file
op.output(
    "write_output",
    output_lines,
    FileSink(Path("output.jsonl")),
)
