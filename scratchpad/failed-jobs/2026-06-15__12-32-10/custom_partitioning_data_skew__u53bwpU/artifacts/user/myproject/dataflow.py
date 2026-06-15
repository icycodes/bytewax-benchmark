"""Bytewax dataflow with custom partitioned source using round-robin strategy."""

from pathlib import Path
from typing import Iterable, List, Optional

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.files import FileSink


class RoundRobinPartition(StatefulSourcePartition[str, int]):
    """A partition that reads lines from a file where line_index % num_partitions == partition_id."""

    def __init__(self, path: Path, partition_id: int, num_partitions: int, resume_state: Optional[int]):
        self._path = path
        self._partition_id = partition_id
        self._num_partitions = num_partitions
        self._next_line_idx = resume_state if resume_state is not None else 0
        self._lines = None
        self._eof = False

    def next_batch(self) -> Iterable[str]:
        if self._eof:
            raise StopIteration()

        if self._lines is None:
            with open(self._path, "r") as f:
                self._lines = f.readlines()

        batch = []
        while self._next_line_idx < len(self._lines):
            if self._next_line_idx % self._num_partitions == self._partition_id:
                line = self._lines[self._next_line_idx].rstrip("\n")
                batch.append(line)
                self._next_line_idx += 1
                if len(batch) >= 1:
                    break
            else:
                self._next_line_idx += 1

        if not batch and self._next_line_idx >= len(self._lines):
            self._eof = True
            raise StopIteration()

        return batch

    def snapshot(self) -> int:
        return self._next_line_idx


class RoundRobinFileSource(FixedPartitionedSource[str, int]):
    """Custom source that distributes file lines across partitions using round-robin."""

    def __init__(self, path: Path, num_partitions: int = 3):
        if not isinstance(path, Path):
            path = Path(path)
        self._path = path
        self._num_partitions = num_partitions

    def list_parts(self) -> List[str]:
        return [str(i) for i in range(self._num_partitions)]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> RoundRobinPartition:
        partition_id = int(for_part)
        return RoundRobinPartition(
            self._path, partition_id, self._num_partitions, resume_state
        )


flow = Dataflow("round_robin_partitioned")
stream = op.input("input", flow, RoundRobinFileSource(Path("input.jsonl"), num_partitions=3))
keyed = op.key_on("add_key", stream, lambda x: x)
op.output("output", keyed, FileSink(Path("output.jsonl")))