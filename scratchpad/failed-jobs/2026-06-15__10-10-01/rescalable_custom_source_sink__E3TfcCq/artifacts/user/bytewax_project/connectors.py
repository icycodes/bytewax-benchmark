"""Custom rescalable file source and sink for Bytewax."""

import os
from typing import Iterable, List, Optional

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


class _FileSourcePartition(StatefulSourcePartition[tuple, int]):
    """Stateful partition that reads lines from a file, tracking line number.

    Yields (filename, line) tuples so the sink can route by filename.
    """

    def __init__(self, filename: str, filepath: str, resume_state: Optional[int]):
        self._filename = filename
        self._filepath = filepath
        self._line_number = resume_state if resume_state is not None else 0
        self._file = None

    def _ensure_file_open(self):
        if self._file is None:
            self._file = open(self._filepath, "r")
            # Seek to the current line number
            for _ in range(self._line_number):
                self._file.readline()

    def next_batch(self) -> Iterable[tuple]:
        self._ensure_file_open()
        batch = []
        for line in self._file:
            self._line_number += 1
            batch.append((self._filename, line.rstrip("\n").rstrip("\r")))
            if len(batch) >= 100:
                break
        return batch

    def snapshot(self) -> int:
        return self._line_number

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None


class CustomFileSource(FixedPartitionedSource[tuple, int]):
    """Reads all .txt files in input_dir, each file as a distinct partition.

    Yields (filename, line) tuples so the sink can route by filename.
    """

    def __init__(self, input_dir: str):
        self._input_dir = input_dir

    def list_parts(self) -> List[str]:
        files = sorted(
            f for f in os.listdir(self._input_dir) if f.endswith(".txt")
        )
        return files

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> StatefulSourcePartition[tuple, int]:
        filepath = os.path.join(self._input_dir, for_part)
        return _FileSourcePartition(for_part, filepath, resume_state)


class _FileSinkPartition(StatefulSinkPartition[tuple, int]):
    """Stateful partition that writes lines to a file, tracking line count.

    On initialization with resume_state, truncates the file to that many
    lines to prevent duplicate writes after recovery.
    """

    def __init__(self, filepath: str, resume_state: Optional[int]):
        self._filepath = filepath
        self._line_count = resume_state if resume_state is not None else 0
        self._file = None
        self._ensure_file_open()

    def _ensure_file_open(self):
        if self._file is None:
            # Read existing file content
            if os.path.exists(self._filepath):
                with open(self._filepath, "r") as f:
                    lines = f.readlines()
                # Truncate to resume_state lines
                keep = lines[: self._line_count]
                with open(self._filepath, "w") as f:
                    f.writelines(keep)
            else:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
            self._file = open(self._filepath, "a")

    def write_batch(self, values: List[tuple]):
        for item in values:
            # item is (filename, line_content)
            _, line_content = item
            self._file.write(line_content + "\n")
            self._line_count += 1
        self._file.flush()
        os.fsync(self._file.fileno())

    def snapshot(self) -> int:
        return self._line_count

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None


class CustomFileSink(FixedPartitionedSink[tuple, int]):
    """Writes items to output_dir, partitioned by filename.

    Each item is expected to be a (filename, line_content) tuple.
    The filename is used as the partition key.

    On the first run, we need to know the expected partitions ahead of time
    since the output directory is empty. We use the source's partition list.
    """

    def __init__(self, output_dir: str, part_keys: List[str]):
        self._output_dir = output_dir
        self._part_keys = part_keys

    def list_parts(self) -> List[str]:
        return list(self._part_keys)

    def part_fn(self, item_key: str) -> int:
        import zlib

        # item_key is the filename (first element of the tuple)
        return zlib.adler32(item_key.encode())

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> StatefulSinkPartition[tuple, int]:
        filepath = os.path.join(self._output_dir, for_part)
        return _FileSinkPartition(filepath, resume_state)
