"""Bytewax windowed word count dataflow.

Usage:
    python dataflow.py <input_file> <output_file>

Input format (JSONL):
    {"time": "2023-01-01T00:05:00Z", "word": "hello"}

Output format (JSONL):
    {"word": "hello", "window_id": "2023-01-01T00:00:00+00:00", "count": 3}
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    count_window,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WINDOW_LENGTH = timedelta(hours=1)
ALIGN_TO = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Custom file sink (writes plain strings, one per line)
# ---------------------------------------------------------------------------


class _JsonlSinkPartition(StatelessSinkPartition):
    def __init__(self, path: str) -> None:
        self._f = open(path, "w", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._f.write(item + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()


class JsonlFileSink(DynamicSink):
    """Write string items to a JSONL file, one item per line."""

    def __init__(self, path: str) -> None:
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        # Only worker 0 writes to avoid duplicate output when running
        # with multiple workers. Other workers write to /dev/null.
        if worker_index == 0:
            return _JsonlSinkPartition(self._path)
        return _JsonlSinkPartition("/dev/null")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_event(line: str):
    """Parse a JSONL line into a (word, datetime) tuple."""
    event = json.loads(line)
    ts = datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
    word = event["word"]
    return (word, ts)


def get_timestamp(item):
    """Return the UTC-aware datetime embedded in each item."""
    _word, ts = item
    return ts


def format_output(item) -> str:
    """Convert (key, (window_id, count)) into a JSONL string."""
    word, (window_id, count) = item
    window_start = ALIGN_TO + window_id * WINDOW_LENGTH
    record = {
        "word": word,
        "window_id": window_start.isoformat(),
        "count": count,
    }
    return json.dumps(record)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------


def build_dataflow(input_path: str, output_path: str) -> Dataflow:
    flow = Dataflow("windowed_word_count")

    # 1. Read raw lines from the input file
    raw = op.input("read", flow, FileSource(input_path))

    # 2. Parse each JSON line into (word, timestamp)
    parsed = op.map("parse", raw, parse_event)

    # 3. Configure event-time clock.
    #    wait_for_system_duration=0 advances the watermark immediately,
    #    which is correct for bounded / batch input.
    clock = EventClock(
        ts_getter=get_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 4. 1-hour tumbling window aligned to 2023-01-01T00:00:00Z
    windower = TumblingWindower(length=WINDOW_LENGTH, align_to=ALIGN_TO)

    # 5. Count occurrences of each word per window.
    #    count_window keys by the provided function; the key is the word.
    windowed = count_window(
        "count",
        up=parsed,
        clock=clock,
        windower=windower,
        key=lambda item: item[0],
    )

    # 6. Format the downstream results as JSON strings
    formatted = op.map("format", windowed.down, format_output)

    # 7. Write to the output file
    op.output("write", formatted, JsonlFileSink(output_path))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    flow = build_dataflow(input_file, output_file)

    from bytewax.testing import run_main
    run_main(flow)
