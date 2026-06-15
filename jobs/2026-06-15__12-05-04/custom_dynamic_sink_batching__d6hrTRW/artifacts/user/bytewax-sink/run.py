import json
import os
from pathlib import Path
from typing import List

from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource
import bytewax.operators as op

# ── constants ────────────────────────────────────────────────────────────────
RECORDS_PER_FILE = 20
OUTPUT_DIR = Path("out")
RUN_ID = os.environ["ZEALT_RUN_ID"]


# ── sink partition ────────────────────────────────────────────────────────────
class RotatingFileSinkPartition(StatelessSinkPartition):
    """Writes items to JSONL files, rotating every RECORDS_PER_FILE records."""

    def __init__(self, worker_index: int) -> None:
        self._worker_index = worker_index
        self._part_number = 0
        self._records_in_current_file = 0
        self._current_file = None
        self._open_next_part()

    # ── private helpers ───────────────────────────────────────────────────────

    def _file_path(self, part: int) -> Path:
        filename = (
            f"output-{RUN_ID}-worker-{self._worker_index}-part-{part}.jsonl"
        )
        return OUTPUT_DIR / filename

    def _open_next_part(self) -> None:
        """Close any open file and open the next part file."""
        if self._current_file is not None:
            self._current_file.close()

        path = self._file_path(self._part_number)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._current_file = open(path, "w", encoding="utf-8")
        self._records_in_current_file = 0

    # ── StatelessSinkPartition interface ──────────────────────────────────────

    def write_batch(self, items: List) -> None:
        """Write a batch of items, rotating files as needed.

        A single batch may span multiple file boundaries.  We consume
        items one-by-one so we can close/open part files at exactly the
        right boundary.
        """
        for item in items:
            # Rotate *before* writing if the current file is already full.
            if self._records_in_current_file >= RECORDS_PER_FILE:
                self._part_number += 1
                self._open_next_part()

            record = json.dumps({"worker": self._worker_index, "value": item})
            self._current_file.write(record + "\n")
            self._records_in_current_file += 1

        # Flush after every batch so data reaches disk promptly.
        self._current_file.flush()

    def close(self) -> None:
        """Flush and close the currently open file when the dataflow finishes."""
        if self._current_file is not None:
            self._current_file.flush()
            self._current_file.close()
            self._current_file = None


# ── dynamic sink ─────────────────────────────────────────────────────────────
class RotatingFileSink(DynamicSink):
    """DynamicSink that creates one RotatingFileSinkPartition per worker."""

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> RotatingFileSinkPartition:
        return RotatingFileSinkPartition(worker_index)


# ── dataflow ──────────────────────────────────────────────────────────────────
flow = Dataflow("file-rotation-sink")

# Emit integers 0..199 from a single-partition source.
stream = op.input("integers", flow, TestingSource(range(200)))

# Each emitted integer is the *value*; the worker index is added in the sink.
op.output("write", stream, RotatingFileSink())
