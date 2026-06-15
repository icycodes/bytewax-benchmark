import csv
import os
from typing import Iterable, List, Optional, Tuple

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

INPUT_FILE = "input.csv"
OUTPUT_FILE = "output.csv"
WINDOW_SIZE = 3


class CSVInputPartition(StatefulSourcePartition[str, int]):
    """A single partition that reads lines from a CSV file."""

    def __init__(self, path: str, resume_state: int = 0):
        self._path = path
        self._lines = self._read_lines()
        self._idx = resume_state

    def _read_lines(self):
        with open(self._path, "r") as f:
            return [line.strip() for line in f if line.strip()]

    def next_batch(self) -> Iterable[str]:
        if self._idx >= len(self._lines):
            raise StopIteration
        line = self._lines[self._idx]
        self._idx += 1
        return [line]

    def snapshot(self) -> int:
        return self._idx


class CSVInputSource(FixedPartitionedSource[str, int]):
    """A fixed-partition source that reads from a CSV file."""

    def __init__(self, path: str):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state):
        return CSVInputPartition(self._path, resume_state or 0)


class CSVOutputPartition(StatelessSinkPartition[str]):
    """A sink partition that appends lines to a CSV file."""

    def __init__(self, path: str):
        self._path = path
        # Ensure the file starts empty
        with open(self._path, "w") as f:
            pass

    def write_batch(self, items: List[str]) -> None:
        with open(self._path, "a") as f:
            for item in items:
                f.write(item + "\n")


class CSVOutputSink(DynamicSink[str]):
    """A dynamic sink that writes lines to a CSV file."""

    def __init__(self, path: str):
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return CSVOutputPartition(self._path)


def _parse_line(line: str) -> Tuple[str, float]:
    """Parse a CSV line into (sensor_id, temperature)."""
    sensor_id, temp_str = line.split(",")
    return (sensor_id, float(temp_str))


def _format_output(item: Tuple[str, Tuple[str, float]]) -> str:
    """Format (key, (sensor_id, moving_average)) as 'sensor_id,moving_average'."""
    key, (sensor_id, avg) = item
    return f"{sensor_id},{avg:.2f}"


def _moving_average(
    state: Optional[Tuple[float, ...]], value: Tuple[str, float]
) -> Tuple[Tuple[float, ...], Tuple[str, float]]:
    """Compute the moving average of the last N readings.

    Args:
        state: The current state (tuple of recent readings), or None if first call.
        value: A (sensor_id, temperature) tuple.

    Returns:
        A 2-tuple of (new_state, (sensor_id, moving_average)).
    """
    sensor_id, temp = value

    if state is None:
        readings: List[float] = []
    else:
        readings = list(state)

    readings.append(temp)
    if len(readings) > WINDOW_SIZE:
        readings = readings[-WINDOW_SIZE:]

    avg = sum(readings) / len(readings)
    return (tuple(readings), (sensor_id, round(avg, 2)))


flow = Dataflow("sensor_moving_average")

# Input: read lines from input.csv
lines = op.input("input", flow, CSVInputSource(INPUT_FILE))

# Parse each line: "sensor_id,temperature" → (sensor_id, temperature)
parsed = op.map("parse", lines, _parse_line)

# Key by sensor_id so stateful_map can maintain per-sensor state
keyed = op.key_on("key_on_sensor", parsed, lambda x: x[0])

# Stateful map: compute moving average per sensor key
with_state = op.stateful_map("moving_average", keyed, _moving_average)

# Format output: (sensor_id, moving_average) → "sensor_id,moving_average"
formatted = op.map("format", with_state, _format_output)

# Output: write to output.csv
op.output("output", formatted, CSVOutputSink(OUTPUT_FILE))
