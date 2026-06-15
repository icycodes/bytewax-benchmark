import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    fold_window,
    join_window,
    WindowMetadata,
)
from bytewax.testing import run_main

# --- Configuration ---
T = 500  # Apdex threshold in ms
TOLERATING_UPPER = 2000  # Upper bound for tolerating zone
WINDOW_LENGTH = timedelta(seconds=10)

# Align windows to epoch so that windows start at :00, :10, :20, etc.
ALIGN_TO = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_input(line: str) -> tuple[str, int, datetime]:
    """Parse a JSON line into (service, response_time_ms, timestamp)."""
    record = json.loads(line)
    ts = datetime.fromisoformat(record["timestamp"])
    return (record["service"], record["response_time_ms"], ts)


def extract_timestamp(item: tuple[str, int, datetime]) -> datetime:
    """Extract the event timestamp from a parsed item."""
    return item[2]


def initial_acc() -> dict:
    """Initial accumulator for Apdex calculation."""
    return {"satisfied": 0, "tolerating": 0, "total": 0}


def folder(acc: dict, item: tuple[str, int, datetime]) -> dict:
    """Fold each item into the accumulator."""
    response_time = item[1]
    acc["total"] += 1
    if response_time <= T:
        acc["satisfied"] += 1
    elif response_time <= TOLERATING_UPPER:
        acc["tolerating"] += 1
    # else: frustrated — contributes nothing to numerator
    return acc


def merger(a: dict, b: dict) -> dict:
    """Merge two accumulators (for late-arriving data)."""
    return {
        "satisfied": a["satisfied"] + b["satisfied"],
        "tolerating": a["tolerating"] + b["tolerating"],
        "total": a["total"] + b["total"],
    }


def calc_apdex(acc: dict) -> float:
    """Calculate Apdex score from the accumulator."""
    if acc["total"] == 0:
        return 0.0
    return (acc["satisfied"] + acc["tolerating"] / 2) / acc["total"]


def build_flow() -> Dataflow:
    flow = Dataflow("apdex_calculator")

    # Read input lines
    input_stream = op.input(
        "read_input", flow, _FileSource(Path("input.jsonl"))
    )

    # Parse JSON and extract (service, response_time_ms, timestamp)
    parsed = op.map("parse", input_stream, parse_input)

    # Key by service for windowing
    keyed = op.key_on("key_by_service", parsed, lambda x: x[0])

    # Configure event-time clock
    clock = EventClock(
        ts_getter=extract_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 10-second tumbling windower
    windower = TumblingWindower(
        length=WINDOW_LENGTH,
        align_to=ALIGN_TO,
    )

    # Fold into windows — returns WindowOut with .down and .meta
    windowed = fold_window(
        "window_apdex",
        keyed,
        clock=clock,
        windower=windower,
        builder=initial_acc,
        folder=folder,
        merger=merger,
    )

    # Join the accumulator results with window metadata
    # windowed.down:  (key, (window_id, accumulator))
    # windowed.meta:  (key, (window_id, WindowMetadata))
    joined = join_window(
        "join_meta",
        clock,
        windower,
        windowed.down,
        windowed.meta,
    )

    # joined.down: (key, ((window_id, acc), (window_id, meta)))
    def format_output(
        item: tuple[str, tuple[tuple[int, dict], tuple[int, WindowMetadata]]]
    ) -> str:
        service = item[0]
        (_win_id, acc), (_win_id2, win_meta) = item[1]
        apdex = round(calc_apdex(acc), 2)
        result = {
            "window_start": win_meta.open_time.isoformat().replace(
                "+00:00", "Z"
            ),
            "service": service,
            "apdex_score": apdex,
        }
        return json.dumps(result)

    formatted = op.map("format", joined.down, format_output)

    # Write to output file
    op.output("write_output", formatted, _FileSink(Path("output.jsonl")))

    return flow


# --- File-based I/O helpers ---

class _FileSource:
    """A Bytewax source that reads lines from a JSONL file."""

    def __init__(self, path: Path):
        self._path = path

    def list_parts(self):
        return [str(self._path)]

    def build_part(self, _step_id, _path, _resume_state):
        with open(self._path) as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    yield stripped


class _FileSink:
    """A Bytewax sink that writes lines to a JSONL file."""

    def __init__(self, path: Path):
        self._path = path

    def list_parts(self):
        return [str(self._path)]

    def build_part(self, _step_id, _path, _resume_state):
        with open(self._path, "w") as f:
            pass  # truncate

        def write(item):
            with open(self._path, "a") as f:
                f.write(item + "\n")

        return write


if __name__ == "__main__":
    flow = build_flow()
    run_main(flow)
