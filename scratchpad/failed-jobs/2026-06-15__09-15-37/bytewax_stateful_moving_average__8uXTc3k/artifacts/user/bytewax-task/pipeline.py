"""Bytewax stateful pipeline that computes a 3-reading moving average
per sensor from input.csv and writes results to output.csv."""

from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSink

# ---------------------------------------------------------------------------
# Custom file source that reads plain CSV lines (no header)
# ---------------------------------------------------------------------------

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class _CSVPartition(StatefulSourcePartition):
    """Reads every line from a plain CSV file (no header row)."""

    def __init__(self, path: Path, resume_state: Optional[int]) -> None:
        self._path = path
        self._start = resume_state or 0
        self._file = open(path, "r")
        # Fast-forward to the resume position (number of lines already consumed).
        for _ in range(self._start):
            self._file.readline()
        self._pos = self._start

    def next_batch(self):
        line = self._file.readline()
        if line == "":
            raise StopIteration
        self._pos += 1
        return [line.rstrip("\n")]

    def snapshot(self) -> int:
        return self._pos

    def close(self) -> None:
        self._file.close()


class PlainCSVSource(FixedPartitionedSource):
    """Single-partition source that emits raw lines from a CSV file."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state):
        return _CSVPartition(self._path, resume_state)


# ---------------------------------------------------------------------------
# Stateful moving-average logic
# ---------------------------------------------------------------------------

WINDOW = 3  # number of readings to keep


def moving_average(
    state: Optional[Tuple[float, ...]], temperature: float
) -> Tuple[Tuple[float, ...], float]:
    """Maintain a sliding window of the last WINDOW readings.

    Args:
        state: Tuple of recent temperature readings (oldest … newest),
               or None on first encounter.
        temperature: Incoming reading.

    Returns:
        (new_state, moving_average) where new_state is a fresh tuple
        (never mutated in-place to stay recovery-safe).
    """
    if state is None:
        state = ()

    # Build a new tuple — do not mutate the existing one.
    new_state: Tuple[float, ...] = (state + (temperature,))[-WINDOW:]
    avg = sum(new_state) / len(new_state)
    return new_state, avg


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("moving_avg")

# 1. Read raw CSV lines.
raw = op.input("csv_in", flow, PlainCSVSource(Path("input.csv")))

# 2. Parse each line into (sensor_id, temperature).
#    op.map expects (item) -> new_item; keys are added in step 3.
parsed = op.map(
    "parse",
    raw,
    lambda line: (line.split(",")[0].strip(), float(line.split(",")[1].strip())),
)

# 3. Key the stream by sensor_id so stateful_map can track per-sensor state.
keyed = op.key_on("key_by_sensor", parsed, lambda pair: pair[0])

# keyed stream items are now (sensor_id, (sensor_id, temperature)); unwrap value.
values = op.map_value("extract_temp", keyed, lambda pair: pair[1])

# 4. Compute the stateful moving average.
averaged = op.stateful_map("moving_avg", values, moving_average)

# 5. Format output as "sensor_id,moving_average".
#    FileSink consumes a keyed stream of (key, str); we format the value
#    and keep the key so the sink can route correctly.
formatted = op.map_value(
    "format",
    averaged,
    lambda avg: f"{avg:.2f}",
)

# Produce the final CSV string as a keyed value so FileSink receives (key, str).
csv_line = op.map(
    "to_csv_line",
    formatted,
    lambda kv: (kv[0], f"{kv[0]},{kv[1]}"),
)

# 6. Write to output.csv (FileSink needs the file to exist first).
output_path = Path("output.csv")
output_path.touch()
op.output("csv_out", csv_line, FileSink(output_path))
