from pathlib import Path
from typing import List, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.files import FileSink


class CustomFilePartition(StatefulSourcePartition[str, int]):
    def __init__(self, path: Path, part_id: int, resume_state: Optional[int]):
        self._path = path
        self._part_id = part_id
        self._line_idx = resume_state if resume_state is not None else 0
        self._f = open(path, "rt")
        # Fast-forward to the saved position/line index
        for _ in range(self._line_idx):
            self._f.readline()

    def next_batch(self) -> List[str]:
        while True:
            line = self._f.readline()
            if not line:
                raise StopIteration()
            
            current_idx = self._line_idx
            self._line_idx += 1
            
            if current_idx % 3 == self._part_id:
                return [line.rstrip("\n")]

    def snapshot(self) -> int:
        return self._line_idx

    def close(self) -> None:
        self._f.close()


class CustomPartitionedSource(FixedPartitionedSource[str, int]):
    def __init__(self, path: Path):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["0", "1", "2"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> CustomFilePartition:
        part_id = int(for_part)
        return CustomFilePartition(self._path, part_id, resume_state)


# Define the dataflow
flow = Dataflow("custom_partitioning_flow")

# Use our custom partitioned source
input_path = Path("input.jsonl")
custom_source = CustomPartitionedSource(input_path)

# Introduce items into the dataflow
stream = op.input("input_step", flow, custom_source)

# Key the lines for the output sink, which requires a (key, value) 2-tuple.
# We can use a constant key since FileSink writes to a single file.
keyed_stream = op.map("key_lines", stream, lambda line: ("key", line))

# Write items out of the dataflow using standard Bytewax output sink FileSink
output_path = Path("output.jsonl")
op.output("output_step", keyed_stream, FileSink(output_path))
