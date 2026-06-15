"""Bytewax dataflow for computing running maximum per key with recovery support."""

import os
from typing import List, Optional

import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class _RecoverableFilePartition(StatefulSourcePartition[str, int]):
    """A partition that reads lines from a file without depending on the
    filename in the partition key. This allows recovery of downstream
    stateful operators across runs with different input files."""

    def __init__(self, path, batch_size, resume_state):
        self._path = path
        self._batch_size = batch_size
        self._resume_state = resume_state
        self._file = None
        self._lines_iter = None

    def next_batch(self):
        if self._file is None:
            self._file = open(self._path, "r")
            # If resuming, skip the already-processed lines
            if self._resume_state is not None:
                for _ in range(self._resume_state):
                    self._file.readline()
            self._lines_iter = self._file

        batch = []
        for _ in range(self._batch_size):
            line = self._file.readline()
            if not line:
                break
            batch.append(line.rstrip("\n"))
        return batch

    def snapshot(self) -> int:
        """Return the number of lines processed so far as resume state."""
        if self._resume_state is None:
            return 0
        return self._resume_state

    def close(self):
        if self._file is not None:
            self._file.close()


class RecoverableFileSource(FixedPartitionedSource[str, int]):
    """A file source that uses a fixed partition key so that downstream
    stateful operators can be recovered across runs with different files.

    Unlike FileSource which includes the file path in the partition key,
    this source always uses a single fixed partition name, allowing the
    stateful_map state to persist across different input files.
    """

    PARTITION_NAME = "single_partition"

    def __init__(self, path, batch_size=1):
        self._path = path
        self._batch_size = batch_size

    def list_parts(self) -> List[str]:
        return [self.PARTITION_NAME]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _RecoverableFilePartition:
        return _RecoverableFilePartition(
            self._path, self._batch_size, resume_state
        )


def get_flow():
    """Build and return the dataflow for running max computation.

    Reads input/output paths from environment variables:
      - BYTEWAX_INPUT_PATH: path to the input CSV file
      - BYTEWAX_OUTPUT_PATH: path to the output file

    Returns:
        A Dataflow object ready to be executed.
    """
    input_path = os.environ["BYTEWAX_INPUT_PATH"]
    output_path = os.environ["BYTEWAX_OUTPUT_PATH"]

    flow = Dataflow("running_max")

    # Read input lines from file using our custom recoverable source
    stream = op.input("input", flow, RecoverableFileSource(input_path))

    # Parse each line: "key,value" -> (key, int(value))
    def parse_line(line):
        parts = line.strip().split(",")
        key = parts[0].strip()
        value = int(parts[1].strip())
        return (key, value)

    parsed = op.map("parse", stream, parse_line)

    # Stateful operator: compute running maximum per key
    def running_max(previous_max, value):
        """Track the running max per key.

        Args:
            previous_max: The max value seen so far for this key, or None.
            value: The new value to compare against.

        Returns:
            A tuple of (new_state, output_value).
        """
        if previous_max is None:
            current_max = value
        else:
            current_max = max(previous_max, value)
        return (current_max, current_max)

    max_stream = op.stateful_map("running_max", parsed, running_max)

    # Format output: (key, running_max) -> (key, "key,running_max")
    # Keep the key for routing, format the value as the output string.
    def format_output(item):
        key, running_max_val = item
        return (key, f"{key},{running_max_val}")

    output_stream = op.map("format_output", max_stream, format_output)

    # Write to output file (FileSink expects string values)
    op.output("output", output_stream, FileSink(output_path))

    return flow