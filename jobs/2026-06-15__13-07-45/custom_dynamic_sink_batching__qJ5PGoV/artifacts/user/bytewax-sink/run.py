import os
import json

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource


class RotatingJsonlPartition(StatelessSinkPartition):
    """A sink partition that writes JSON Lines files with rotation every 20 records."""

    MAX_RECORDS_PER_FILE = 20

    def __init__(self, worker_index: int, run_id: str):
        self.worker_index = worker_index
        self.run_id = run_id
        self.part_number = 0
        self.records_in_file = 0
        self.current_file = None
        self._open_next_file()

    def _open_next_file(self):
        """Close the current file (if any) and open the next part file."""
        if self.current_file is not None:
            self.current_file.close()

        filename = f"output-{self.run_id}-worker-{self.worker_index}-part-{self.part_number}.jsonl"
        out_dir = "out"
        os.makedirs(out_dir, exist_ok=True)
        filepath = os.path.join(out_dir, filename)
        self.current_file = open(filepath, "w")
        self.records_in_file = 0

    def _write_one(self, item):
        """Write a single record to the current file, rotating if necessary."""
        if self.records_in_file >= self.MAX_RECORDS_PER_FILE:
            self.part_number += 1
            self._open_next_file()

        record = {"worker": self.worker_index, "value": item}
        self.current_file.write(json.dumps(record) + "\n")
        self.records_in_file += 1

    def write_batch(self, items):
        """Write a batch of items, handling rotation across multiple files if needed."""
        for item in items:
            self._write_one(item)

    def close(self):
        """Close the current file on shutdown."""
        if self.current_file is not None:
            self.current_file.close()
            self.current_file = None


class RotatingJsonlSink(DynamicSink):
    """A dynamic sink that creates a RotatingJsonlPartition per worker."""

    def __init__(self, run_id: str):
        self.run_id = run_id

    def build(self, step_id: str, worker_index: int, worker_count: int) -> RotatingJsonlPartition:
        return RotatingJsonlPartition(worker_index, self.run_id)


def flow() -> Dataflow:
    run_id = os.environ["ZEALT_RUN_ID"]

    dataflow = Dataflow("rotating-jsonl-sink")

    # Input: 200 integers from 0 to 199
    inp = op.input("inp", dataflow, TestingSource(range(200)))

    # Output: write to rotating JSONL files
    op.output("out", inp, RotatingJsonlSink(run_id))

    return dataflow
