"""Bytewax dataflow with custom round-robin partitioned source.

Reads from input.jsonl using a custom FixedPartitionedSource that
distributes lines across 3 partitions using round-robin logic.
Writes all processed lines to output.jsonl.
"""

import json
import os
from typing import Iterable, List, Optional

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

import bytewax.operators as op

INPUT_FILE = os.path.join(os.path.dirname(__file__), "input.jsonl")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output.jsonl")
NUM_PARTITIONS = 3


class RoundRobinPartition(StatefulSourcePartition[str, int]):
    """A single partition that yields lines assigned to it via round-robin.

    The partition reads the entire file but only yields lines whose
    index modulo NUM_PARTITIONS equals this partition's index.
    """

    def __init__(self, part_idx: int):
        self._part_idx = part_idx
        self._lines: Optional[List[str]] = None
        self._next_line_idx = 0
        self._done = False

    def _ensure_loaded(self):
        if self._lines is None:
            with open(INPUT_FILE, "r") as f:
                self._lines = f.readlines()

    def next_batch(self) -> Iterable[str]:
        if self._done:
            raise StopIteration

        self._ensure_loaded()
        assert self._lines is not None

        batch: List[str] = []
        while self._next_line_idx < len(self._lines):
            line_idx = self._next_line_idx
            self._next_line_idx += 1
            if line_idx % NUM_PARTITIONS == self._part_idx:
                batch.append(self._lines[line_idx].rstrip("\n").rstrip("\r"))

        self._done = True
        if not batch:
            raise StopIteration
        return batch

    def snapshot(self) -> int:
        return self._next_line_idx


class RoundRobinSource(FixedPartitionedSource[str, int]):
    """Custom source with 3 partitions using round-robin distribution.

    Partitions: "0", "1", "2"
    Line 0 -> partition "0", line 1 -> partition "1", line 2 -> partition "2",
    line 3 -> partition "0", and so on.
    """

    def list_parts(self) -> List[str]:
        return [str(i) for i in range(NUM_PARTITIONS)]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> StatefulSourcePartition[str, int]:
        part_idx = int(for_part)
        partition = RoundRobinPartition(part_idx)
        if resume_state is not None:
            partition._next_line_idx = resume_state
        return partition


class JSONLOutputPartition(StatelessSinkPartition[str]):
    """Writes each batch of items as lines to output.jsonl."""

    def __init__(self):
        # Open in append mode since multiple workers may write
        self._file = open(OUTPUT_FILE, "a")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._file.write(item + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


class JSONLSink(DynamicSink[str]):
    """Dynamic sink that writes to output.jsonl."""

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[str]:
        return JSONLOutputPartition()


flow = Dataflow("round_robin_flow")

# Clear output file before starting
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

# Input: custom round-robin partitioned source
lines = op.input("inp", flow, RoundRobinSource())

# Output: write to output.jsonl
op.output("out", lines, JSONLSink())
