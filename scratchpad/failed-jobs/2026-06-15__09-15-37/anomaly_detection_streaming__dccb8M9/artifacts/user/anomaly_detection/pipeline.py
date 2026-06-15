"""Anomaly detection pipeline using Bytewax sliding windows over IoT sensor data.

Usage
-----
Initialise the SQLite recovery partitions (once, before first run):

    mkdir -p ./recovery_dir
    python -m bytewax.recovery ./recovery_dir 1

Run (or resume) the pipeline:

    python -m bytewax.run pipeline:flow -r ./recovery_dir -s 1 -b 0

Input  : /home/user/anomaly_detection/data.jsonl
Output : /home/user/anomaly_detection/anomalies.jsonl
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    SlidingWindower,
    fold_window,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_PATH = Path("/home/user/anomaly_detection/data.jsonl")
OUTPUT_PATH = Path("/home/user/anomaly_detection/anomalies.jsonl")

# Sliding window parameters
WINDOW_LENGTH = timedelta(seconds=60)
WINDOW_STEP = timedelta(seconds=20)

# Alignment anchor for SlidingWindower – must be timezone-aware UTC.
ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# How long to wait in *system* time after the latest event before advancing
# the watermark.  Zero means the watermark advances purely from event time;
# on EOF the runtime automatically closes all remaining open windows.
WAIT_FOR_SYSTEM = timedelta(seconds=0)

# ---------------------------------------------------------------------------
# Picklable per-window accumulator (Welford's online algorithm)
# ---------------------------------------------------------------------------


@dataclass
class WindowAccumulator:
    """Fully picklable accumulator for online mean and population std-dev.

    Stores every raw (timestamp, value) pair so that outliers can be
    reported with the *final* window statistics once the window closes.

    Uses Welford's numerically stable online algorithm for variance so
    that the accumulator stays fully picklable with no external state.
    """

    readings: List[Tuple[str, float]] = field(default_factory=list)
    n: int = 0
    mean: float = 0.0
    # Running sum of squared deviations (Welford M2 term)
    M2: float = 0.0

    def add(self, timestamp: str, value: float) -> "WindowAccumulator":
        """Incorporate a new reading into the running statistics (in-place)."""
        self.readings.append((timestamp, value))
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.M2 += delta * delta2
        return self

    @property
    def std_dev(self) -> float:
        """Population standard deviation; returns 0.0 when n < 2."""
        if self.n < 2:
            return 0.0
        return math.sqrt(self.M2 / self.n)

    def merge(self, other: "WindowAccumulator") -> "WindowAccumulator":
        """Combine two partial accumulators produced by different workers."""
        merged = WindowAccumulator()
        for ts, val in self.readings + other.readings:
            merged.add(ts, val)
        return merged


# ---------------------------------------------------------------------------
# Custom DynamicSink – writes plain-string items to a JSONL file
# ---------------------------------------------------------------------------


class _JsonlSinkPartition(StatelessSinkPartition[str]):
    """Appends one JSON string per line to the target file."""

    def __init__(self, path: Path) -> None:
        self._file = path.open("a", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._file.write(item + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


class JsonlFileSink(DynamicSink[str]):
    """Append-mode JSONL sink.

    Uses :class:`DynamicSink` so that items from the dataflow do not need
    to be wrapped in ``(key, value)`` pairs – plain string items work
    directly.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        # Ensure the parent directory and the file both exist.
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _JsonlSinkPartition:
        return _JsonlSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Pure helper functions (module-level → fully picklable)
# ---------------------------------------------------------------------------


def _parse_line(line: str) -> Optional[dict]:
    """Parse a JSONL line; return ``None`` to discard blanks/malformed lines."""
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _get_event_timestamp(record: dict) -> datetime:
    """Extract a timezone-aware UTC :class:`~datetime.datetime` from a record."""
    ts_str: str = record["timestamp"]
    # Normalise trailing 'Z' for Python < 3.11 where fromisoformat rejects it.
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _build_accumulator() -> WindowAccumulator:
    """Builder called by :func:`fold_window` to initialise each new window."""
    return WindowAccumulator()


def _fold_reading(acc: WindowAccumulator, record: dict) -> WindowAccumulator:
    """Folder called by :func:`fold_window` for each record in the window."""
    return acc.add(record["timestamp"], record["value"])


def _merge_accumulators(
    a: WindowAccumulator, b: WindowAccumulator
) -> WindowAccumulator:
    """Merger called by :func:`fold_window` when combining partial windows."""
    return a.merge(b)


def _detect_anomalies(
    item: Tuple[str, Tuple[int, WindowAccumulator]],
) -> List[str]:
    """Emit anomaly JSON strings for a closed window.

    :arg item: ``(sensor_id, (window_id, accumulator))`` tuple emitted by
               ``win_out.down``.
    :returns: A list of JSON strings, one per detected outlier.  Empty if
              the standard deviation is zero (no spread → no outliers) or if
              no reading exceeds the 3-sigma threshold.
    """
    sensor_id, (_win_id, acc) = item

    # Not enough data or no spread → cannot have outliers.
    if acc.n == 0 or acc.std_dev == 0.0:
        return []

    mean = acc.mean
    std = acc.std_dev
    threshold = 3.0 * std
    anomalies: List[str] = []

    for timestamp, value in acc.readings:
        if abs(value - mean) > threshold:
            record = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "value": value,
                "mean": round(mean, 6),
                "std_dev": round(std, 6),
            }
            anomalies.append(json.dumps(record))

    return anomalies


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

# Truncate / create the output file so each fresh run starts cleanly.
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.write_text("", encoding="utf-8")

flow = Dataflow("anomaly_detection")

# Step 1 – Read raw text lines from the JSONL input file.
raw_lines: op.Stream[str] = op.input(
    "file_input", flow, FileSource(INPUT_PATH)
)

# Step 2 – Parse JSON; malformed / blank lines are silently dropped.
parsed: op.Stream[dict] = op.filter_map(
    "parse_json", raw_lines, _parse_line
)

# Step 3 – Key by sensor_id.
#           Bytewax requires *string* keys for all stateful operators.
keyed: op.Stream[Tuple[str, dict]] = op.key_on(
    "key_by_sensor",
    parsed,
    lambda rec: str(rec["sensor_id"]),
)

# Step 4 – Define the event-time clock.
#           The watermark advances from event timestamps; on EOF the
#           runtime advances to UTC_MAX so all open windows are closed.
clock: EventClock[dict] = EventClock(
    ts_getter=_get_event_timestamp,
    wait_for_system_duration=WAIT_FOR_SYSTEM,
)

# Step 5 – Define the sliding window (60 s length, 20 s step).
windower: SlidingWindower = SlidingWindower(
    length=WINDOW_LENGTH,
    offset=WINDOW_STEP,
    align_to=ALIGN_TO,
)

# Step 6 – Accumulate readings per sensor per window.
#           win_out.down carries  (sensor_id, (window_id, WindowAccumulator))
#           win_out.late carries  (sensor_id, (window_id, dict))  [late data]
#           win_out.meta carries  (sensor_id, (window_id, WindowMetadata))
win_out = fold_window(
    "sliding_window",
    keyed,
    clock,
    windower,
    _build_accumulator,
    _fold_reading,
    _merge_accumulators,
)

# Step 7 – Detect outliers in each closed window and serialise to JSON strings.
anomaly_lines: op.Stream[str] = op.flat_map(
    "detect_anomalies",
    win_out.down,
    _detect_anomalies,
)

# Step 8 – Write anomaly JSON strings to the output JSONL file.
op.output("file_output", anomaly_lines, JsonlFileSink(OUTPUT_PATH))
