"""
Bytewax (v0.21.1) sliding-window outlier detector with SQLite recovery support.

Reads JSON-lines from input.jsonl, applies event-time sliding windows (60 s length,
30 s offset) per sensor_id, and emits mean + population stddev for each closed window
to output.jsonl.

Run:
    python -m bytewax.run pipeline:flow -r ./recovery_dir -s 1 -b 0
"""

import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.operators.windowing import EventClock, SlidingWindower, WindowMetadata
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# ---------------------------------------------------------------------------
# Input source – reads lines from a JSONL file, resuming from a line offset.
# The resume state is a plain int (fully picklable) so recovery snapshots work.
# ---------------------------------------------------------------------------

_INPUT_FILE = os.path.join(os.path.dirname(__file__), "input.jsonl")


class _JsonlPartition(StatefulSourcePartition):
    """One-partition stateful reader; state = number of lines already consumed."""

    def __init__(self, path: str, resume_offset: int) -> None:
        self._path = path
        self._offset = resume_offset
        self._fh = open(path, "r", encoding="utf-8")
        # fast-forward to the resume point
        for _ in range(resume_offset):
            self._fh.readline()

    # ---- StatefulSourcePartition interface ---------------------------------

    def next_batch(self) -> List[str]:
        line = self._fh.readline()
        if not line:
            raise StopIteration
        self._offset += 1
        return [line.rstrip("\n")]

    def snapshot(self) -> int:
        """Return the number of lines consumed – a plain int, fully picklable."""
        return self._offset

    def close(self) -> None:
        self._fh.close()


class JsonlSource(FixedPartitionedSource):
    """Single-partition fixed source backed by a JSONL file."""

    def __init__(self, path: str) -> None:
        self._path = path

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _JsonlPartition:
        offset = resume_state if resume_state is not None else 0
        return _JsonlPartition(self._path, offset)


# ---------------------------------------------------------------------------
# Output sink – appends JSON lines to output.jsonl.
# DynamicSink is stateless, which is fine; the important picklable state lives
# in the windowing accumulators above.
# ---------------------------------------------------------------------------

_OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output.jsonl")


class _JsonlSinkPartition(StatelessSinkPartition):
    def __init__(self, path: str) -> None:
        self._fh = open(path, "w", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._fh.write(item + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class JsonlSink(DynamicSink):
    def __init__(self, path: str) -> None:
        self._path = path

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _JsonlSinkPartition:
        return _JsonlSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Windowing accumulator helpers
# All three callables are module-level *named functions* (not lambdas) so they
# are picklable by the standard pickle module, as required for SQLite recovery.
# ---------------------------------------------------------------------------


def _builder() -> List[float]:
    """Return a fresh, empty accumulator (a plain list – fully picklable)."""
    return []


def _folder(acc: List[float], event: dict) -> List[float]:
    """Append the temperature from *event* into *acc*."""
    acc.append(float(event["temp"]))
    return acc


def _merger(acc1: List[float], acc2: List[float]) -> List[float]:
    """Merge two accumulators (used when windows overlap and need combining)."""
    return acc1 + acc2


# ---------------------------------------------------------------------------
# Event-time extractor – also a module-level named function for picklability.
# ---------------------------------------------------------------------------


def _get_event_time(event: dict) -> datetime:
    """Parse the ISO-8601 'time' field and return an aware UTC datetime."""
    raw = event["time"]
    # Replace trailing 'Z' with '+00:00' so fromisoformat() works on Py < 3.11
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Output formatter: joins the meta stream with the accumulator stream so that
# each emitted record has both WindowMetadata and the temperature list.
# ---------------------------------------------------------------------------

# Bytewax fold_window returns a WindowOut whose .down stream emits
#   (sensor_id, (window_id, acc))
# and whose .meta stream emits
#   (sensor_id, (window_id, WindowMetadata))
#
# We join them on (sensor_id, window_id) using op.join, then format.


def _extract_win_id_and_acc(
    item: Tuple[int, List[float]]
) -> Tuple[str, List[float]]:
    """Re-key by window_id (cast to str) so op.join can match on it."""
    win_id, acc = item
    return (str(win_id), acc)


def _extract_win_id_and_meta(
    item: Tuple[int, WindowMetadata]
) -> Tuple[str, WindowMetadata]:
    win_id, meta = item
    return (str(win_id), meta)


def _format_record(
    sensor_id_and_payload: Tuple[str, Tuple[List[float], WindowMetadata]]
) -> str:
    """Compute stats and serialise to a JSON string."""
    # After the join the stream is keyed by sensor_id; the value is the joined
    # tuple (acc, meta) produced by op.join.
    sensor_id, (acc, meta) = sensor_id_and_payload
    temps: List[float] = acc
    n = len(temps)
    mean = sum(temps) / n if n > 0 else 0.0
    variance = sum((t - mean) ** 2 for t in temps) / n if n > 0 else 0.0
    stddev = math.sqrt(variance)

    def _iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return json.dumps(
        {
            "sensor_id": sensor_id,
            "window_start": _iso(meta.open_time),
            "window_end": _iso(meta.close_time),
            "mean": round(mean, 6),
            "stddev": round(stddev, 6),
        }
    )


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

_ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

flow = Dataflow("sensor_outlier_detector")

# 1. Read raw lines from the JSONL file
raw = op.input("input", flow, JsonlSource(_INPUT_FILE))

# 2. Parse each line into a dict
parsed = op.map("parse_json", raw, json.loads)

# 3. Key the stream by sensor_id (must be a str)
keyed = op.key_on("key_by_sensor", parsed, lambda event: event["sensor_id"])

# 4. Define event clock (uses the 'time' field; no extra system-time waiting
#    because this is a batch/file source and we want immediate window close)
clock = EventClock(
    ts_getter=_get_event_time,
    wait_for_system_duration=timedelta(seconds=0),
)

# 5. Sliding window: 60 s long, advances every 30 s
windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=_ALIGN_TO,
)

# 6. Accumulate temperatures in each window
window_out = win.fold_window(
    "sliding_window",
    keyed,
    clock,
    windower,
    _builder,
    _folder,
    _merger,
)

# window_out.down  : KeyedStream[(sensor_id, (win_id, List[float]))]
# window_out.meta  : KeyedStream[(sensor_id, (win_id, WindowMetadata))]

# 7. Re-key both streams by (sensor_id + window_id) so we can join them.
#    Strategy: flatten sensor_id into the sub-key, then use op.join.

# Re-key down stream: new key = "sensor_id|win_id", value = acc
def _rekey_down(
    item: Tuple[str, Tuple[int, List[float]]]
) -> Tuple[str, List[float]]:
    sensor_id, (win_id, acc) = item
    return (f"{sensor_id}|{win_id}", acc)


# Re-key meta stream: new key = "sensor_id|win_id", value = WindowMetadata
def _rekey_meta(
    item: Tuple[str, Tuple[int, WindowMetadata]]
) -> Tuple[str, WindowMetadata]:
    sensor_id, (win_id, meta) = item
    return (f"{sensor_id}|{win_id}", meta)


down_rekeyed = op.map("rekey_down", window_out.down, _rekey_down)
meta_rekeyed = op.map("rekey_meta", window_out.meta, _rekey_meta)

# 8. Join accumulator with metadata on the composite key
joined = op.join("join_win_meta", down_rekeyed, meta_rekeyed)

# joined stream: (composite_key, (acc, meta))
# composite_key = "sensor_id|win_id"
# Unwrap sensor_id for the output record
def _unwrap_and_format(
    item: Tuple[str, Tuple[List[float], WindowMetadata]]
) -> str:
    composite_key, (acc, meta) = item
    sensor_id = composite_key.split("|", 1)[0]
    temps: List[float] = acc
    n = len(temps)
    mean = sum(temps) / n if n > 0 else 0.0
    variance = sum((t - mean) ** 2 for t in temps) / n if n > 0 else 0.0
    stddev = math.sqrt(variance)

    def _iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return json.dumps(
        {
            "sensor_id": sensor_id,
            "window_start": _iso(meta.open_time),
            "window_end": _iso(meta.close_time),
            "mean": round(mean, 6),
            "stddev": round(stddev, 6),
        }
    )


formatted = op.map("format_output", joined, _unwrap_and_format)

# 9. Write to output.jsonl
op.output("output", formatted, JsonlSink(_OUTPUT_FILE))
