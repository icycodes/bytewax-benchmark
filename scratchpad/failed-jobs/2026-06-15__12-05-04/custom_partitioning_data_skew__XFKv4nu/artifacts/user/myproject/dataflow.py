"""
Bytewax dataflow that reads input.jsonl via a custom FixedPartitionedSource
with a round-robin strategy across exactly 3 partitions, then writes every
line to output.jsonl.

Partition assignment:
  line index % 3 == 0  →  partition "0"
  line index % 3 == 1  →  partition "1"
  line index % 3 == 2  →  partition "2"
"""

import json
from pathlib import Path
from typing import Iterable, List, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.files import FileSink

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INPUT_FILE = Path(__file__).parent / "input.jsonl"
OUTPUT_FILE = Path(__file__).parent / "output.jsonl"
NUM_PARTITIONS = 3
PARTITION_KEYS = [str(i) for i in range(NUM_PARTITIONS)]


# ---------------------------------------------------------------------------
# Stateful partition – owns one slice of the file (every Nth line)
# ---------------------------------------------------------------------------

class RoundRobinFilePartition(StatefulSourcePartition):
    """Reads lines from a JSONL file that belong to *this* partition.

    The partition index is determined by ``line_index % NUM_PARTITIONS``.
    ``resume_state`` is the *next* line index (global) this partition
    should read from, so recovery after failure skips already-emitted lines.
    """

    def __init__(self, path: Path, partition_id: int, resume_state: Optional[int]):
        self._path = path
        self._partition_id = partition_id

        # resume_state holds the next global line index to read from.
        # None means start from the beginning.
        self._next_line_idx: int = resume_state if resume_state is not None else 0

        # Fast-forward the file iterator to the correct starting point.
        self._file = open(path, "r", encoding="utf-8")
        self._current_line_idx = 0  # tracks global line position in the file

        # Advance past lines that were already emitted in a previous run.
        # We skip lines whose index is either:
        #   (a) not assigned to this partition, OR
        #   (b) already consumed (index < resume_state).
        # Because we read line-by-line we can't truly seek, so we simply
        # advance through lines we won't emit.
        while self._current_line_idx < self._next_line_idx:
            line = self._file.readline()
            if not line:
                break  # EOF reached before resume point
            self._current_line_idx += 1

    # ------------------------------------------------------------------
    # StatefulSourcePartition interface
    # ------------------------------------------------------------------

    def next_batch(self) -> Iterable[str]:
        """Return the next line belonging to this partition, or raise StopIteration."""
        while True:
            line = self._file.readline()
            if not line:
                # End of file – signal completion to Bytewax.
                raise StopIteration()

            idx = self._current_line_idx
            self._current_line_idx += 1

            if idx % NUM_PARTITIONS == self._partition_id:
                # This line belongs to us; update the resume pointer PAST it.
                self._next_line_idx = self._current_line_idx
                return [line.rstrip("\n")]
            # Otherwise skip and loop to find the next owned line.

    def snapshot(self) -> int:
        """Return the global line index of the next line to emit."""
        return self._next_line_idx

    def close(self) -> None:
        self._file.close()


# ---------------------------------------------------------------------------
# FixedPartitionedSource – declares the 3 partitions and builds them
# ---------------------------------------------------------------------------

class RoundRobinJsonlSource(FixedPartitionedSource):
    """JSONL source that spreads lines across NUM_PARTITIONS via round-robin."""

    def __init__(self, path: Path):
        self._path = path

    def list_parts(self) -> List[str]:
        """All workers collectively own every partition key."""
        return PARTITION_KEYS

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> RoundRobinFilePartition:
        partition_id = int(for_part)
        return RoundRobinFilePartition(self._path, partition_id, resume_state)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

# Ensure the output file exists so FileSink can open it.
OUTPUT_FILE.touch()

flow = Dataflow("round_robin_jsonl")

# Read from the custom partitioned source; each item is a raw JSON string.
lines = op.input("read_jsonl", flow, RoundRobinJsonlSource(INPUT_FILE))

# FileSink (a FixedPartitionedSink) expects (key, value) 2-tuples for routing.
# We use a fixed key ("out") so all items land in the single output partition.
keyed = op.map("key_lines", lines, lambda line: ("out", line))

# Write every line as-is to output.jsonl.
op.output("write_jsonl", keyed, FileSink(OUTPUT_FILE))
