from collections import deque
from pathlib import Path
from typing import List, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op

INPUT_PATH = Path("input.csv")
OUTPUT_PATH = Path("output.csv")


class CSVSourcePartition(StatefulSourcePartition[Tuple[str, str], int]):
    """Reads CSV lines from a file, returning (sensor_id, temperature) tuples."""

    def __init__(self, lines: List[str], resume_state: Optional[int]):
        self._lines = lines
        self._idx = 0 if resume_state is None else resume_state
        self._exhausted = False

    def next_batch(self) -> List[Tuple[str, str]]:
        if self._exhausted:
            raise StopIteration()

        batch: List[Tuple[str, str]] = []
        while self._idx < len(self._lines):
            line = self._lines[self._idx].strip()
            self._idx += 1
            if not line:
                continue
            parts = line.split(",")
            if len(parts) == 2:
                batch.append((parts[0].strip(), parts[1].strip()))
        self._exhausted = True
        return batch

    def snapshot(self) -> int:
        return self._idx


class CSVSource(FixedPartitionedSource[Tuple[str, str], int]):
    """Fixed-partitioned source that reads from input.csv."""

    def __init__(self, path: Path):
        self._lines = path.read_text().splitlines()

    def list_parts(self) -> List[str]:
        return ["csv"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> CSVSourcePartition:
        return CSVSourcePartition(self._lines, resume_state)


class CSVWriteSinkPartition(StatelessSinkPartition[Tuple[str, str]]):
    """Writes (sensor_id, moving_average) lines to a file."""

    def __init__(self, path: Path):
        self._path = path
        # Clear the file on first write
        self._path.write_text("")

    def write_batch(self, items: List[Tuple[str, str]]) -> None:
        with open(self._path, "a") as f:
            for sensor_id, avg in items:
                f.write(f"{sensor_id},{avg}\n")


class CSVWriteSink(DynamicSink[Tuple[str, str]]):
    """Dynamic sink that writes to output.csv."""

    def __init__(self, path: Path):
        self._path = path

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> CSVWriteSinkPartition:
        return CSVWriteSinkPartition(self._path)


def moving_average_mapper(
    state: Optional[List[float]], value: Tuple[str, str]
) -> Tuple[Optional[List[float]], str]:
    """
    Stateful mapper that computes the moving average of the last 3 readings.

    Args:
        state: A list of the last up-to-3 temperature readings, or None.
        value: A (sensor_id, temperature_string) tuple from the upstream
               keyed stream.

    Returns:
        A tuple of (new_state, moving_average_string).
        new_state is a new list (not mutated) for recovery compatibility.
    """
    _, temp_str = value
    temp = float(temp_str)

    if state is None:
        new_state = [temp]
    else:
        # Create a new list (do not mutate in-place)
        new_state = list(state)
        new_state.append(temp)
        if len(new_state) > 3:
            new_state.pop(0)

    avg = sum(new_state) / len(new_state)
    avg_str = f"{avg:.2f}"

    return (new_state, avg_str)


flow = Dataflow("sensor_moving_average")

# 1. Read from input.csv
raw_lines = op.input("read_csv", flow, CSVSource(INPUT_PATH))

# 2. Key by sensor_id (first element of the tuple)
keyed = op.key_on("key_by_sensor", raw_lines, lambda t: t[0])

# 3. Compute stateful moving average
averaged = op.stateful_map("moving_avg", keyed, moving_average_mapper)

# 4. Write to output.csv
op.output("write_output", averaged, CSVWriteSink(OUTPUT_PATH))
