"""Bytewax dataflow with custom rescalable file source and sink."""

from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


# ---------------------------------------------------------------------------
# Custom File Source
# ---------------------------------------------------------------------------

class _FileSourcePartition(StatefulSourcePartition):
    """Reads lines from a single file, tracking line position as state.

    Yields ``(filename, line_content)`` tuples so the downstream
    sink can route items to the correct output file.
    """

    def __init__(self, filepath: Path, resume_state: Optional[int]):
        self._filepath = filepath
        self._filename = filepath.name
        # resume_state is the next line number to read (0-based).
        # If None, start from the beginning (line 0).
        self._next_line = resume_state if resume_state is not None else 0
        self._lines: List[str] = []
        self._eof = False
        self._loaded = False

    def _load_lines(self) -> None:
        """Read all lines from the file once."""
        if self._loaded:
            return
        with open(self._filepath, "r") as f:
            self._lines = [line.rstrip("\n") for line in f.readlines()]
        self._loaded = True

    def next_batch(self) -> Iterable:
        self._load_lines()

        if self._next_line >= len(self._lines):
            self._eof = True
            raise StopIteration()

        # Yield one (filename, line) tuple per batch so snapshot
        # stays accurate and the sink can route correctly.
        line = self._lines[self._next_line]
        self._next_line += 1
        return [(self._filename, line)]

    def next_awake(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> int:
        return self._next_line

    def close(self) -> None:
        pass


class CustomFileSource(FixedPartitionedSource):
    """Source that reads .txt files from a directory.

    Each file is a separate partition. Items yielded are
    ``(filename, line_content)`` tuples so the sink can route
    them to the correct output file.
    """

    def __init__(self, input_dir: str):
        self._input_dir = Path(input_dir)

    def list_parts(self) -> List[str]:
        parts = sorted(
            f.name
            for f in self._input_dir.iterdir()
            if f.is_file() and f.suffix == ".txt"
        )
        return parts

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> _FileSourcePartition:
        filepath = self._input_dir / for_part
        return _FileSourcePartition(filepath, resume_state)


# ---------------------------------------------------------------------------
# Custom File Sink
# ---------------------------------------------------------------------------

class _FileSinkPartition(StatefulSinkPartition):
    """Writes lines to a single file, tracking lines written as state."""

    def __init__(self, filepath: Path, resume_state: Optional[int]):
        self._filepath = filepath
        # resume_state is the number of lines already committed.
        # If None, start from scratch (0 lines written).
        self._lines_written = resume_state if resume_state is not None else 0
        self._pending_lines: int = 0

        # On recovery, truncate the file to the committed number of lines
        # to remove any partially-written or duplicate data.
        self._truncate_to(self._lines_written)

    def _truncate_to(self, num_lines: int) -> None:
        """Truncate the file so it contains exactly *num_lines* lines."""
        if not self._filepath.exists():
            # Nothing to truncate; file doesn't exist yet.
            return

        with open(self._filepath, "r") as f:
            existing_lines = f.readlines()

        # Keep only the first num_lines lines.
        kept = existing_lines[:num_lines]
        with open(self._filepath, "w") as f:
            f.writelines(kept)

    def write_batch(self, values: List[str]) -> None:
        with open(self._filepath, "a") as f:
            for value in values:
                f.write(value + "\n")
        self._pending_lines += len(values)

    def snapshot(self) -> int:
        self._lines_written += self._pending_lines
        self._pending_lines = 0
        return self._lines_written

    def close(self) -> None:
        pass


class CustomFileSink(FixedPartitionedSink):
    """Sink that writes lines to files in a directory.

    The partition key is the filename. Items in the stream must be
    ``(filename, line_content)`` tuples; the filename routes the item
    to the correct partition, and the line_content is written.
    """

    def __init__(self, output_dir: str):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        # Cache the partition list so part_fn can route deterministically.
        input_dir = self._output_dir.parent / "input_data"
        self._part_keys = sorted(
            f.name
            for f in input_dir.iterdir()
            if f.is_file() and f.suffix == ".txt"
        )

    def list_parts(self) -> List[str]:
        # Must list the same set of partition keys as the source so
        # every source partition has a corresponding sink partition.
        return list(self._part_keys)

    def part_fn(self, item_key: str) -> int:
        # Route each item to the partition whose key matches the
        # item key. We return an integer such that
        # `integer % len(global_parts)` equals the index of the key
        # in the sorted partition list.
        idx = self._part_keys.index(item_key)
        # The framework selects partition via `global_parts[hash % n]`.
        # Return idx + k*n for any k≥0 to guarantee correct routing.
        # Using idx directly works since idx < n, so idx % n == idx.
        return idx

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> _FileSinkPartition:
        filepath = self._output_dir / for_part
        return _FileSinkPartition(filepath, resume_state)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("file_uppercase")

# Read (filename, line_content) tuples from input_data/
inp = op.input("file_input", flow, CustomFileSource("/home/user/bytewax_project/input_data"))

# Convert line content to uppercase (source yields (filename, line) tuples)
upper = op.map("to_upper", inp, lambda item: (item[0], item[1].upper()))

# Write to output_data/
op.output("file_output", upper, CustomFileSink("/home/user/bytewax_project/output_data"))