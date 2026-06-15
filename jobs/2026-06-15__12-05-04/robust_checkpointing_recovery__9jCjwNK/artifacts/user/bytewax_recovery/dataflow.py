"""
Bytewax stateful dataflow: computes running maximum per key from a CSV input.

Environment variables (required):
    INPUT_FILE  – path to input CSV file with `key,value` lines
    OUTPUT_FILE – path to output file; results written as `key,running_max`

The dataflow is designed to be launched via:
    python -m bytewax.run dataflow -r <recovery_dir>

State recovery is handled automatically by Bytewax when the -r flag points
to a directory containing pre-initialised SQLite recovery partitions.

Design note on custom connectors
----------------------------------
Bytewax's built-in `FileSource` and `FileSink` encode the file path into the
recovery partition key.  When run with `-r`, Bytewax tries to resume every
step from the saved partition keys.  If the file path changes between runs the
key no longer matches and Bytewax raises a runtime error.

`StableFileSource` and `StableFileSink` below use a *constant* partition key
(`"input"` / `"output"`) so that the recovery layer can always locate the
partition, regardless of which files are being processed.  The file-offset
resume state is intentionally not used: each run processes a new, complete
batch file so we always read / write from scratch.  Only the `stateful_map`
running-maximum state needs to survive between runs – and Bytewax handles that
automatically through its SQLite recovery snapshots.
"""

import os
from pathlib import Path
from typing import List, Optional

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
import bytewax.operators as op
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Configuration via environment variables (set by run.sh before invocation)
# ---------------------------------------------------------------------------
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]


# ---------------------------------------------------------------------------
# Custom file source with a stable partition key
# ---------------------------------------------------------------------------
class _StableFilePartition(StatefulSourcePartition):
    """Read all lines of *path* from the beginning; resume state is ignored."""

    def __init__(self, path: Path, batch_size: int) -> None:
        self._f = open(path, "rt")
        self._batch_size = batch_size

    def next_batch(self) -> List[str]:
        lines = []
        for _ in range(self._batch_size):
            line = self._f.readline()
            if not line:
                break
            lines.append(line.rstrip("\n"))
        if not lines:
            raise StopIteration
        return lines

    def snapshot(self) -> None:
        # Always start fresh from byte 0 on the next run – no offset to save.
        return None

    def close(self) -> None:
        self._f.close()


class StableFileSource(FixedPartitionedSource):
    """File source with a constant partition key.

    Using a fixed key (`"input"`) means Bytewax's recovery system can always
    match the stored snapshot to this step, even when the input file changes
    between runs.  The file is always read from byte 0.
    """

    _PART_KEY = "input"

    def __init__(self, path: Path, batch_size: int = 1000) -> None:
        self._path = Path(path)
        self._batch_size = batch_size

    def list_parts(self) -> List[str]:
        return [self._PART_KEY]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[None]
    ) -> _StableFilePartition:
        return _StableFilePartition(self._path, self._batch_size)


# ---------------------------------------------------------------------------
# Custom file sink with a stable partition key
# ---------------------------------------------------------------------------
class _StableFileSinkPartition(StatefulSinkPartition):
    """Write string values to *path*, truncating the file on every open."""

    def __init__(self, path: Path) -> None:
        # Always truncate – each run produces a fresh output file.
        self._f = open(path, "wt")

    def write_batch(self, values: List[str]) -> None:
        for value in values:
            self._f.write(value)
            self._f.write("\n")
        self._f.flush()

    def snapshot(self) -> None:
        # No offset tracking needed; we always truncate on open.
        return None

    def close(self) -> None:
        self._f.close()


class StableFileSink(FixedPartitionedSink):
    """File sink with a constant partition key.

    Using a fixed key (`"output"`) prevents the path-mismatch error that the
    built-in `FileSink` raises when the output path changes between runs.
    The output file is always truncated at the start of each run.

    Upstream items must be `(key, value_str)` tuples; only the string value
    is written to the file.
    """

    _PART_KEY = "output"

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def list_parts(self) -> List[str]:
        return [self._PART_KEY]

    def part_fn(self, item_key: str) -> int:
        return 0

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[None]
    ) -> _StableFileSinkPartition:
        return _StableFileSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------
flow = Dataflow("running_max")

# 1. Read lines from the input CSV file using the stable-key source
lines = op.input("file_input", flow, StableFileSource(INPUT_FILE))

# 2. Parse each "key,value" line into a keyed (key, int_value) tuple.
#    Lines that are empty, comments, or malformed are silently dropped.
def parse_line(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(",", 1)
    if len(parts) != 2:
        return None
    key, raw_value = parts[0].strip(), parts[1].strip()
    try:
        return (key, int(raw_value))
    except ValueError:
        return None

parsed = op.filter_map("parse_csv", lines, parse_line)

# 3. Stateful running-maximum operator.
#    State  = the maximum value seen so far for this key (None on first event).
#    Emit   = the updated maximum.
def running_max(state, value: int):
    new_max = value if state is None else max(state, value)
    return (new_max, new_max)

keyed_max = op.stateful_map("running_max", parsed, running_max)

# 4. Format output as keyed (key, "key,running_max") tuples.
#    StableFileSink routes by item_key and writes the string value.
formatted = op.map(
    "format_output",
    keyed_max,
    lambda kv: (kv[0], f"{kv[0]},{kv[1]}"),
)

# 5. Write to the output file
op.output("file_output", formatted, StableFileSink(Path(OUTPUT_FILE)))
