"""
Bytewax dataflow for sliding-window outlier detection on sensor temperatures.

Computes per-sensor sliding window statistics (mean, population stddev)
with SQLite-based recovery support. All state is picklable.
"""

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    SlidingWindower,
    WindowMetadata,
    WindowOut,
    fold_window,
)
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition


# ---------------------------------------------------------------------------
# Pure-Python accumulator state (must be picklable)
# ---------------------------------------------------------------------------

class TempAccumulator:
    """Accumulator for temperature values within a window.

    Stores individual values to compute mean and population stddev on close.
    Must be a pure Python class (no lambdas, no unpicklable types) so that
    Bytewax's SQLite recovery snapshots can serialize it.
    """

    def __init__(self):
        self.values: List[float] = []

    def add(self, temp: float) -> None:
        self.values.append(temp)

    def merge(self, other: "TempAccumulator") -> "TempAccumulator":
        merged = TempAccumulator()
        merged.values = self.values + other.values
        return merged

    def compute_stats(self) -> Tuple[float, float]:
        """Return (mean, population_stddev)."""
        n = len(self.values)
        if n == 0:
            return (0.0, 0.0)
        mean = sum(self.values) / n
        variance = sum((v - mean) ** 2 for v in self.values) / n
        stddev = math.sqrt(variance)
        return (mean, stddev)


# ---------------------------------------------------------------------------
# Named functions for fold_window (must be picklable, so no lambdas)
# ---------------------------------------------------------------------------

def builder() -> TempAccumulator:
    """Create an empty accumulator for a new window."""
    return TempAccumulator()


def folder(acc: TempAccumulator, value: Tuple[str, float, datetime]) -> TempAccumulator:
    """Fold a new temperature value into the accumulator.

    The value V is (sensor_id, temp, parsed_datetime) from the keyed stream.
    """
    _, temp, _ = value
    acc.add(temp)
    return acc


def merger(a: TempAccumulator, b: TempAccumulator) -> TempAccumulator:
    """Merge two accumulators when windows merge."""
    return a.merge(b)


# ---------------------------------------------------------------------------
# Timestamp extraction (named function, not lambda)
# ---------------------------------------------------------------------------

def extract_timestamp(item: Tuple[str, float, datetime]) -> datetime:
    """Extract the event-time timestamp from a stream item.

    The stream value V is (sensor_id, temp, parsed_datetime).
    The ts_getter receives V and must return the event-time datetime.
    """
    _, _, dt = item
    return dt


# ---------------------------------------------------------------------------
# File source (proper Source subclass)
# ---------------------------------------------------------------------------

class JSONLinesPartition(StatefulSourcePartition[str, int]):
    """Reads JSON lines from a file, maintaining read position for recovery."""

    def __init__(self, path: str, resume_pos: Optional[int]):
        self._path = path
        self._fh = open(path, "r", encoding="utf-8")
        if resume_pos is not None:
            self._fh.seek(resume_pos)
        self._done = False

    def next_batch(self) -> Iterable[str]:
        if self._done:
            raise StopIteration()
        lines = []
        for line in self._fh:
            line = line.strip()
            if line:
                lines.append(line)
            # Yield in small batches for better cooperative multitasking
            if len(lines) >= 100:
                return lines
        self._done = True
        return lines

    def snapshot(self) -> int:
        """Return the current file position for recovery snapshots."""
        return self._fh.tell()

    def close(self) -> None:
        self._fh.close()


class JSONLinesSource(FixedPartitionedSource[str, int]):
    """Fixed-partition source that reads a single JSON lines file."""

    def __init__(self, path: str):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state: Optional[int]) -> JSONLinesPartition:
        return JSONLinesPartition(self._path, resume_state)


# ---------------------------------------------------------------------------
# JSON Lines file sink
# ---------------------------------------------------------------------------

class JSONLinesSinkPartition(StatelessSinkPartition[str]):
    """Writes items as JSON lines to a file."""

    def __init__(self, path: str):
        self._fh = open(path, "a", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._fh.write(item + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class JSONLinesSink(DynamicSink[str]):
    """Dynamic sink factory for JSON lines output."""

    def __init__(self, path: str):
        self._path = path

    def build(self, _step_id: str, _worker_index: int, _worker_count: int) -> JSONLinesSinkPartition:
        return JSONLinesSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Helper: parse input JSON line
# ---------------------------------------------------------------------------

def parse_line(line: str) -> Tuple[str, Tuple[str, float]]:
    """Parse a JSON line into (sensor_id, (sensor_id, temp)).

    Returns a key-value pair for the keyed stream. The key is sensor_id
    (must be a string for recovery). The value is a tuple of (sensor_id, temp)
    that the fold_window folder will receive.

    Note: The timestamp is extracted separately by the EventClock's ts_getter,
    which receives the full value tuple. We embed the parsed datetime in the
    value for the clock to use.
    """
    record = json.loads(line)
    sensor_id = record["sensor_id"]
    temp = float(record["temp"])
    dt = datetime.fromisoformat(record["time"].replace("Z", "+00:00"))
    return (sensor_id, (sensor_id, temp, dt))


# ---------------------------------------------------------------------------
# Helper: format window output as JSON line
# ---------------------------------------------------------------------------

def format_output(item: Tuple[str, Tuple[int, TempAccumulator, WindowMetadata]]) -> str:
    """Format a closed window item as a JSON line string.

    Input: (sensor_id, (window_id, accumulator, window_metadata))
    Output: JSON line string with sensor_id, window_start, window_end, mean, stddev
    """
    sensor_id, (_, acc, meta) = item
    mean, stddev = acc.compute_stats()
    result = {
        "sensor_id": sensor_id,
        "window_start": meta.open_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": meta.close_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mean": round(mean, 4),
        "stddev": round(stddev, 4),
    }
    return json.dumps(result)


# ---------------------------------------------------------------------------
# Join flattener (named function, not lambda)
# ---------------------------------------------------------------------------

def flatten_join(
    item: Tuple[str, Tuple[Tuple[int, TempAccumulator], Tuple[int, WindowMetadata]]],
) -> Tuple[str, Tuple[int, TempAccumulator, WindowMetadata]]:
    """Flatten the join result from nested tuples to a flat tuple."""
    sensor_id, ((wid, acc), (_, meta)) = item
    return (sensor_id, (wid, acc, meta))


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

def build_flow() -> Dataflow:
    """Build and return the Bytewax dataflow."""

    flow = Dataflow("sensor_sliding_window_outlier")

    # 1. Read input from input.jsonl
    raw = op.input("read_input", flow, JSONLinesSource("input.jsonl"))

    # 2. Parse JSON lines into (sensor_id, (sensor_id, temp, parsed_datetime))
    keyed = op.map("parse_json", raw, parse_line)

    # 3. Define the event-time clock
    clock = EventClock(
        ts_getter=extract_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 4. Define the sliding windower: 60s length, 30s step, aligned to 2026-01-01T00:00:00Z
    windower = SlidingWindower(
        length=timedelta(seconds=60),
        offset=timedelta(seconds=30),
        align_to=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )

    # 5. Fold temperature values into accumulators per window
    windowed: WindowOut = fold_window(
        "fold_temps",
        keyed,
        clock,
        windower,
        builder=builder,
        folder=folder,
        merger=merger,
    )

    # 6. Join the down stream (accumulator) with the meta stream (window metadata)
    #    down:  (sensor_id, (window_id, accumulator))
    #    meta:  (sensor_id, (window_id, WindowMetadata))
    joined = op.join("join_meta", windowed.down, windowed.meta)

    #    joined: (sensor_id, ((window_id, accumulator), (window_id, metadata)))
    #    Flatten to: (sensor_id, (window_id, accumulator, metadata))
    flattened = op.map("flatten_join", joined, flatten_join)

    # 7. Format as JSON lines
    json_lines = op.map("format_output", flattened, format_output)

    # 8. Write to output.jsonl
    op.output("write_output", json_lines, JSONLinesSink("output.jsonl"))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

flow = build_flow()
