"""
Custom rescalable file source and sink built on Bytewax's
FixedPartitionedSource / FixedPartitionedSink abstractions.

Source
------
* Scans ``input_dir`` for ``.txt`` files; each file is one partition.
* Yields ``(filename, line_content)`` tuples so the sink can route each
  item back to the correct output file.
* ``resume_state`` is the *next* line number to read (0-based), so
  after a crash Bytewax re-builds the partition starting at exactly
  the right line – no duplicates, no gaps.

Sink
----
* Receives ``(filename, line_content)`` pairs.
* On initialisation (or recovery) it truncates the output file to the
  number of lines recorded in ``resume_state``, removing any partially
  written data from a previous failed run.
* ``resume_state`` is the number of lines *successfully committed* so
  far (i.e. what ``snapshot()`` returns after writing N lines is N).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

class _FileSourcePartition(StatefulSourcePartition[Tuple[str, str], int]):
    """Read one ``.txt`` file line by line.

    Parameters
    ----------
    filepath:
        Absolute path to the file being read.
    filename:
        Bare filename (e.g. ``"greetings.txt"``); emitted as the first
        element of every output tuple so the sink can route it.
    resume_state:
        Line number to *start* reading from (0 = beginning of file).
        ``None`` is treated identically to 0.
    """

    def __init__(
        self,
        filepath: Path,
        filename: str,
        resume_state: Optional[int],
    ) -> None:
        self._filepath = filepath
        self._filename = filename
        # next_line is the 0-based index of the line we will read next.
        self._next_line: int = resume_state if resume_state is not None else 0
        self._fh = filepath.open("r", encoding="utf-8")
        # Seek forward to the resume position by skipping lines.
        for _ in range(self._next_line):
            self._fh.readline()
        self._done = False

    # ------------------------------------------------------------------
    # StatefulSourcePartition protocol
    # ------------------------------------------------------------------

    def next_batch(self) -> Iterable[Tuple[str, str]]:
        """Return the next available line, or raise StopIteration when done."""
        if self._done:
            raise StopIteration()

        raw = self._fh.readline()
        if raw == "":
            # EOF
            self._done = True
            raise StopIteration()

        line = raw.rstrip("\n")
        self._next_line += 1
        return [(self._filename, line)]

    def snapshot(self) -> int:
        """Return the line number that the *next* call to ``next_batch`` will read."""
        return self._next_line

    def close(self) -> None:
        self._fh.close()


class CustomFileSource(FixedPartitionedSource[Tuple[str, str], int]):
    """Read every ``.txt`` file in ``input_dir`` as a separate partition.

    Parameters
    ----------
    input_dir:
        Directory containing the ``.txt`` files to read.
    """

    def __init__(self, input_dir: str | Path) -> None:
        self._input_dir = Path(input_dir)

    def list_parts(self) -> List[str]:
        """Return the filenames of all ``.txt`` files in ``input_dir``."""
        return sorted(
            p.name
            for p in self._input_dir.iterdir()
            if p.is_file() and p.suffix == ".txt"
        )

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> _FileSourcePartition:
        filepath = self._input_dir / for_part
        return _FileSourcePartition(filepath, for_part, resume_state)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

class _FileSinkPartition(StatefulSinkPartition[str, int]):
    """Write lines to a single output file.

    On construction the file is truncated to the number of lines
    recorded in ``resume_state`` so that a re-run after a crash does
    not produce duplicate output.

    Parameters
    ----------
    filepath:
        Absolute path to the output file.
    resume_state:
        Number of lines that were *already committed* before this
        execution.  ``None`` means the file is being written for the
        first time.
    """

    def __init__(self, filepath: Path, resume_state: Optional[int]) -> None:
        self._filepath = filepath
        committed_lines: int = resume_state if resume_state is not None else 0

        # ----------------------------------------------------------------
        # Truncate to the committed checkpoint to remove any partially
        # written data from a previous failed execution.
        # ----------------------------------------------------------------
        self._truncate_to_lines(committed_lines)

        self._lines_written: int = committed_lines
        # Open in append mode; we have already removed any excess data above.
        self._fh = filepath.open("a", encoding="utf-8")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _truncate_to_lines(self, n: int) -> None:
        """Keep only the first *n* lines of the output file."""
        if not self._filepath.exists():
            # Nothing to truncate; the file will be created on first write.
            return

        with self._filepath.open("r", encoding="utf-8") as f:
            lines = f.readlines()

        kept = lines[:n]
        with self._filepath.open("w", encoding="utf-8") as f:
            f.writelines(kept)

    # ------------------------------------------------------------------
    # StatefulSinkPartition protocol
    # ------------------------------------------------------------------

    def write_batch(self, values: List[str]) -> None:
        """Append each value as a separate line."""
        for value in values:
            self._fh.write(value + "\n")
        self._fh.flush()
        self._lines_written += len(values)

    def snapshot(self) -> int:
        """Return the total number of lines successfully written so far."""
        return self._lines_written

    def close(self) -> None:
        self._fh.close()


class CustomFileSink(FixedPartitionedSink[str, int]):
    """Write ``(filename, line)`` pairs to files inside ``output_dir``.

    Each unique ``filename`` key is mapped to its own partition so that
    output from different source files never interleaves.

    Parameters
    ----------
    output_dir:
        Directory where output files will be written.  Created if it
        does not already exist.
    input_dir:
        Directory of the source files.  Used to discover the fixed set
        of partition keys so that ``list_parts()`` is consistent with
        the source.
    """

    def __init__(self, output_dir: str | Path, input_dir: str | Path) -> None:
        self._output_dir = Path(output_dir)
        self._input_dir = Path(input_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def list_parts(self) -> List[str]:
        """Mirror the source partition list so routing is deterministic."""
        return sorted(
            p.name
            for p in self._input_dir.iterdir()
            if p.is_file() and p.suffix == ".txt"
        )

    def part_fn(self, item_key: str) -> int:
        """Route each ``(filename, line)`` pair to the partition *named* filename.

        Bytewax selects a partition by computing
        ``sorted_global_parts[part_fn(key) % len(sorted_global_parts)]``.
        Because our partition names are exactly the filenames we use as
        keys, we return the index of ``item_key`` within the sorted
        partition list so the modulo operation resolves to the correct
        partition on every worker configuration.
        """
        parts = self.list_parts()          # already sorted
        try:
            return parts.index(item_key)
        except ValueError:
            # Fallback: stable adler32 hash (should not happen in normal use)
            from zlib import adler32
            return adler32(item_key.encode())

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> _FileSinkPartition:
        filepath = self._output_dir / for_part
        return _FileSinkPartition(filepath, resume_state)
