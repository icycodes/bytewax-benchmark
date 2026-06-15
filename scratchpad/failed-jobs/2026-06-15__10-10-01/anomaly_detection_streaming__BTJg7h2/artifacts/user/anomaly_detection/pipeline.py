"""
Bytewax pipeline for anomaly detection in streaming IoT sensor data.

Reads sensor readings from a JSONL file, groups by sensor_id,
applies a sliding window (60s length, 20s step) over event time,
calculates mean and std_dev per window, and outputs outliers
(value > mean + 3*std_dev or value < mean - 3*std_dev).

Supports SQLite-based recovery for fault tolerance.
"""

import json
import math
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    SlidingWindower,
    WindowMetadata,
    fold_window,
)


# ---------------------------------------------------------------------------
# Input source – reads a JSONL file line by line
# ---------------------------------------------------------------------------

class JSONLPartition(StatefulSourcePartition[dict, int]):
    """A stateful partition that reads lines from a JSONL file.

    Resume state is the line index (0-based) to resume from.
    """

    def __init__(self, path: str, resume_state: Optional[int] = None):
        self._path = path
        self._line_idx = resume_state if resume_state is not None else 0
        self._lines: Optional[List[str]] = None

    def _load_lines(self):
        if self._lines is None:
            with open(self._path, "r") as f:
                self._lines = [line.strip() for line in f if line.strip()]

    def next_batch(self) -> List[dict]:
        self._load_lines()
        if self._line_idx >= len(self._lines):
            raise StopIteration
        line = self._lines[self._line_idx]
        self._line_idx += 1
        return [json.loads(line)]

    def snapshot(self) -> int:
        return self._line_idx


class JSONLSource(FixedPartitionedSource[dict, int]):
    """Fixed-partition source that reads a JSONL file.

    Uses a single partition (index 0) so all data is processed in order.
    """

    def __init__(self, path: str):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["0"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> JSONLPartition:
        return JSONLPartition(self._path, resume_state)


# ---------------------------------------------------------------------------
# Output sink – writes JSONL to a file
# ---------------------------------------------------------------------------

class JSONLSinkPartition(StatelessSinkPartition[dict]):
    def __init__(self, path: str):
        self._file = open(path, "a")

    def write_batch(self, items: List[dict]) -> None:
        for item in items:
            self._file.write(json.dumps(item) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


class JSONLSink(DynamicSink[dict]):
    def __init__(self, path: str):
        self._path = path

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> JSONLSinkPartition:
        return JSONLSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Window state – picklable accumulator for fold_window
# ---------------------------------------------------------------------------

class WindowState:
    """Accumulator for a sliding window: stores all (value, timestamp, sensor_id)
    tuples so we can compute mean, std_dev and detect outliers when the window
    closes.
    """

    def __init__(self):
        self.readings: List[Tuple[float, datetime, str]] = []

    def add(self, value: float, ts: datetime, sensor_id: str):
        self.readings.append((value, ts, sensor_id))


def window_builder() -> WindowState:
    """Create an empty window state."""
    return WindowState()


def window_folder(state: WindowState, event: dict) -> WindowState:
    """Fold an event into the window state."""
    ts = parse_timestamp(event["timestamp"])
    state.add(event["value"], ts, event["sensor_id"])
    return state


def window_merger(a: WindowState, b: WindowState) -> WindowState:
    """Merge two window states (for session windows; included for completeness)."""
    merged = WindowState()
    merged.readings = a.readings + b.readings
    return merged


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_timestamp(ts_str: str) -> datetime:
    """Parse an ISO-8601 timestamp string to a timezone-aware datetime."""
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def ts_getter(event: dict) -> datetime:
    """Extract the event timestamp for the EventClock."""
    return parse_timestamp(event["timestamp"])


def key_on_sensor(event: dict) -> str:
    """Extract the sensor_id as the grouping key (must be a string)."""
    return event["sensor_id"]


def detect_outliers(
    window_result: Tuple[str, Tuple[int, WindowState]],
) -> List[dict]:
    """For a completed window, compute mean/std_dev and yield outlier events.

    Input: (sensor_id, (window_id, WindowState))
    Output: list of anomaly dicts
    """
    sensor_id, (_window_id, state) = window_result
    readings = state.readings

    if len(readings) < 2:
        return []

    values = [r[0] for r in readings]
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(variance)

    if std_dev == 0:
        return []

    threshold = 3 * std_dev
    anomalies = []
    for value, ts, sid in readings:
        if value > mean + threshold or value < mean - threshold:
            anomalies.append(
                {
                    "sensor_id": sid,
                    "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "value": value,
                    "mean": round(mean, 6),
                    "std_dev": round(std_dev, 6),
                }
            )
    return anomalies


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

INPUT_PATH = "/home/user/anomaly_detection/data.jsonl"
OUTPUT_PATH = "/home/user/anomaly_detection/anomalies.jsonl"


def build_flow() -> Dataflow:
    flow = Dataflow("anomaly_detection")

    # 1. Read from JSONL file
    stream = op.input("read_jsonl", flow, JSONLSource(INPUT_PATH))

    # 2. Key by sensor_id (keys must be strings for stateful operations)
    keyed = op.key_on("key_by_sensor", stream, key_on_sensor)

    # 3. Apply sliding window: 60s length, 20s offset (step)
    clock = EventClock(
        ts_getter=ts_getter,
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = SlidingWindower(
        length=timedelta(seconds=60),
        offset=timedelta(seconds=20),
        align_to=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    windowed = fold_window(
        "sliding_window",
        keyed,
        clock=clock,
        windower=windower,
        builder=window_builder,
        folder=window_folder,
        merger=window_merger,
    )

    # 4. Detect outliers from completed windows and flatten
    anomalies = op.flat_map("detect_outliers", windowed.down, detect_outliers)

    # 5. Write anomalies to JSONL file
    op.output("write_anomalies", anomalies, JSONLSink(OUTPUT_PATH))

    return flow


# ---------------------------------------------------------------------------
# Module-level flow for bytewax.run
# ---------------------------------------------------------------------------

flow = build_flow()
