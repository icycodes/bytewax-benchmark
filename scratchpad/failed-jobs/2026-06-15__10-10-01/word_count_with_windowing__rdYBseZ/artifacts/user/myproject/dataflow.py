"""Bytewax dataflow for windowed word count using event time processing."""

import json
import sys
from datetime import datetime, timedelta, timezone
from typing import List

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    WindowMetadata,
    count_window,
)
from bytewax.testing import run_main


class JSONLSource(DynamicSource):
    """Source that reads a JSON Lines file and yields parsed events."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSourcePartition:
        return JSONLPartition(self.filepath)


class JSONLPartition(StatelessSourcePartition):
    """Partition that reads all lines from a JSONL file."""

    def __init__(self, filepath: str):
        self._lines = None
        self._index = 0
        with open(filepath, "r") as f:
            self._lines = [line.strip() for line in f if line.strip()]

    def next_batch(self) -> List[dict]:
        if self._lines is None:
            raise StopIteration

        batch = []
        while self._index < len(self._lines):
            line = self._lines[self._index]
            self._index += 1
            try:
                event = json.loads(line)
                batch.append(event)
            except json.JSONDecodeError:
                continue
            if len(batch) >= 100:
                return batch

        self._lines = None
        if batch:
            return batch
        raise StopIteration


class JSONLSink(DynamicSink):
    """Sink that writes items as JSON Lines to a file."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition:
        return JSONLSinkPartition(self.filepath)


class JSONLSinkPartition(StatelessSinkPartition):
    """Partition that writes items as JSON Lines."""

    def __init__(self, filepath: str):
        self._file = open(filepath, "a")

    def write_batch(self, items: List[dict]) -> None:
        for item in items:
            self._file.write(json.dumps(item) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


def parse_iso8601_to_utc(ts_string: str) -> datetime:
    """Parse an ISO8601 string into a UTC datetime.

    Handles both 'Z' suffix and '+00:00' offset formats.
    """
    ts_string = ts_string.strip()
    # Handle 'Z' suffix
    if ts_string.endswith("Z"):
        ts_string = ts_string[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts_string)
    # Ensure UTC timezone
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def run(input_file: str, output_file: str) -> None:
    """Build and run the windowed word count dataflow."""

    flow = Dataflow("windowed_word_count")

    # 1. Read input — raw dicts from JSONL
    stream = op.input("read_input", flow, JSONLSource(input_file))

    # 2. Configure event time clock: extract timestamp from each event
    def ts_getter(event: dict) -> datetime:
        return parse_iso8601_to_utc(event["time"])

    clock = EventClock(
        ts_getter=ts_getter,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 3. Configure 1-hour tumbling window aligned to 2023-01-01T00:00:00Z
    windower = TumblingWindower(
        length=timedelta(hours=1),
        align_to=datetime(2023, 1, 1, tzinfo=timezone.utc),
    )

    # 4. Count words within windows
    #    count_window internally keys by the word, so we pass raw events
    #    and use the key parameter to extract the word.
    windowed = count_window(
        step_id="count_words",
        up=stream,
        clock=clock,
        windower=windower,
        key=lambda event: event["word"],
    )

    # 5. Build windower logic to resolve window_id integers to open times
    windower_logic = windower.build("count_words")

    # 6. Format output: (word, (window_id_int, count)) -> dict
    def format_output(item: tuple) -> dict:
        word, (window_id_int, count) = item
        window_meta = windower_logic._metadata_for(window_id_int)
        window_start = window_meta.open_time.isoformat()
        return {
            "word": word,
            "window_id": window_start,
            "count": count,
        }

    formatted = op.map("format_output", windowed.down, format_output)

    # 7. Write output
    op.output("write_output", formatted, JSONLSink(output_file))

    # Run the dataflow
    run_main(flow)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    run(sys.argv[1], sys.argv[2])
