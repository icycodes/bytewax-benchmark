import os
from pathlib import Path
from typing import List, Optional, Tuple, Iterable, Iterator
from itertools import islice

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

# Helper functions for line reading and batching
def _strip_n(s: str) -> str:
    return s.rstrip("\n")

def _readlines(f) -> Iterator[str]:
    while True:
        line = f.readline()
        if len(line) <= 0:
            break
        yield line

def batch(ib: Iterable[str], batch_size: int) -> Iterator[List[str]]:
    it = iter(ib)
    while True:
        b = list(islice(it, batch_size))
        if len(b) <= 0:
            return
        yield b

# Custom Source Partition with robust resume_state handling
class CustomFilePartition(StatefulSourcePartition[str, Tuple[str, int]]):
    def __init__(self, path: Path, batch_size: int, resume_state: Optional[Tuple[str, int]]):
        self._path = path
        self._f = open(path, "rt")
        if resume_state is not None:
            old_path_str, offset = resume_state
            # Only resume if the file path is the same
            if old_path_str == str(path):
                self._f.seek(offset)
        it = map(_strip_n, _readlines(self._f))
        self._batcher = batch(it, batch_size)

    def next_batch(self) -> List[str]:
        try:
            return next(self._batcher)
        except StopIteration:
            raise StopIteration

    def snapshot(self) -> Tuple[str, int]:
        return (str(self._path), self._f.tell())

    def close(self) -> None:
        self._f.close()

# Custom Source that inherits from FixedPartitionedSource
class CustomFileSource(FixedPartitionedSource[str, Tuple[str, int]]):
    def __init__(self, path: Path, batch_size: int = 1000):
        self._path = Path(path)
        self._batch_size = batch_size

    def list_parts(self) -> List[str]:
        if self._path.exists():
            return ["single_file_partition"]
        return []

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[Tuple[str, int]]
    ) -> CustomFilePartition:
        return CustomFilePartition(self._path, self._batch_size, resume_state)

# Custom Sink Partition with robust resume_state handling
class CustomFileSinkPartition(StatefulSinkPartition[str, Tuple[str, int]]):
    def __init__(self, path: Path, resume_state: Optional[Tuple[str, int]], end: str = "\n"):
        self._path = path
        self._end = end
        
        resume_offset = 0
        if resume_state is not None:
            old_path_str, offset = resume_state
            # Only resume writing if the file path is the exact same
            if old_path_str == str(path):
                resume_offset = offset
        
        # Open file in append/write mode and truncate to the resume_offset
        self._f = open(path, "at")
        self._f.seek(resume_offset)
        self._f.truncate()

    def write_batch(self, values: List[str]) -> None:
        for value in values:
            self._f.write(value)
            self._f.write(self._end)
        self._f.flush()
        os.fsync(self._f.fileno())

    def snapshot(self) -> Tuple[str, int]:
        return (str(self._path), self._f.tell())

    def close(self) -> None:
        self._f.close()

# Custom Sink that inherits from FixedPartitionedSink
class CustomFileSink(FixedPartitionedSink[str, Tuple[str, int]]):
    def __init__(self, path: Path, end: str = "\n"):
        self._path = Path(path)
        self._end = end

    def list_parts(self) -> List[str]:
        return ["single_file_sink"]

    def part_fn(self, item_key: str) -> int:
        return 0

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[Tuple[str, int]]
    ) -> CustomFileSinkPartition:
        return CustomFileSinkPartition(self._path, resume_state, self._end)

# Parsing function for input lines
def parse_line(line: str) -> Optional[Tuple[str, int]]:
    line = line.strip()
    if not line:
        return None
    try:
        key, val_str = line.split(",", 1)
        return key, int(val_str)
    except Exception:
        return None

# Stateful operator logic for running maximum
def update_max(state: Optional[int], value: int) -> Tuple[Optional[int], int]:
    if state is None:
        new_max = value
    else:
        new_max = max(state, value)
    return new_max, new_max

# Build the dataflow
input_file_path = os.environ.get("INPUT_FILE")
output_file_path = os.environ.get("OUTPUT_FILE")

if not input_file_path or not output_file_path:
    raise ValueError("INPUT_FILE and OUTPUT_FILE environment variables must be set")

flow = Dataflow("running_max_flow")

# Read lines from the custom file source
lines = op.input("inp", flow, CustomFileSource(Path(input_file_path)))

# Parse lines to (key, value) pairs
parsed = op.filter_map("parse", lines, parse_line)

# Maintain stateful running max per key
running_max = op.stateful_map("running_max", parsed, update_max)

# Format to (key, "key,running_max") for the partitioned sink
formatted = op.map("format", running_max, lambda item: (item[0], f"{item[0]},{item[1]}"))

# Output to the custom file sink
op.output("out", formatted, CustomFileSink(Path(output_file_path)))
