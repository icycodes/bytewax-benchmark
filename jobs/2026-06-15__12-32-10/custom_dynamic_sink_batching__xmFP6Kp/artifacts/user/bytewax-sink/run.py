"""Bytewax dataflow with custom DynamicSink that rotates JSONL files."""

import json
import os
from typing import List

from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource

import bytewax.operators as op

RECORDS_PER_FILE = 20


class RotatingFilePartition(StatelessSinkPartition[int]):
    """A partition that writes JSONL files with rotation at RECORDS_PER_FILE records."""

    def __init__(self, run_id: str, worker_index: int):
        self._run_id = run_id
        self._worker_index = worker_index
        self._part_number = 0
        self._records_in_current_file = 0
        self._file = None

    def _file_path(self, part_number: int) -> str:
        return os.path.join(
            "out",
            f"output-{self._run_id}-worker-{self._worker_index}-part-{part_number}.jsonl",
        )

    def _ensure_file_open(self):
        """Lazily open the current file if it's not already open."""
        if self._file is None:
            path = self._file_path(self._part_number)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._file = open(path, "a")  # noqa: SIM115
            self._records_in_current_file = 0

    def _close_file(self):
        if self._file is not None:
            self._file.close()
            self._file = None

    def write_batch(self, items: List[int]) -> None:
        remaining = list(items)
        while remaining:
            self._ensure_file_open()
            space_left = RECORDS_PER_FILE - self._records_in_current_file
            to_write = remaining[:space_left]
            remaining = remaining[space_left:]

            for value in to_write:
                record = {"worker": self._worker_index, "value": value}
                self._file.write(json.dumps(record) + "\n")
                self._records_in_current_file += 1

            # If current file is full, rotate to next part
            if self._records_in_current_file >= RECORDS_PER_FILE:
                self._close_file()
                self._part_number += 1

    def close(self) -> None:
        self._close_file()


class RotatingFileSink(DynamicSink[int]):
    """A DynamicSink that creates a RotatingFilePartition per worker."""

    def __init__(self, run_id: str):
        self._run_id = run_id

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> RotatingFilePartition:
        return RotatingFilePartition(self._run_id, worker_index)


# Read run-id from environment variable
run_id = os.environ["ZEALT_RUN_ID"]

# Build the dataflow
flow = Dataflow("rotating_file_sink")
nums = op.input("nums", flow, TestingSource(range(200)))
redistributed = op.redistribute("redistribute", nums)
op.output("out", redistributed, RotatingFileSink(run_id))