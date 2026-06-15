"""
Real-Time Apdex Score Calculation with Bytewax
===============================================
Reads JSON lines from input.jsonl, calculates the Apdex score per service
using a 10-second tumbling event-time window, and writes results to output.jsonl.

Apdex threshold T = 500 ms
  Satisfied  : response_time_ms <= 500
  Tolerating : 500 < response_time_ms <= 2000
  Frustrated : response_time_ms > 2000

Formula: (Satisfied + Tolerating / 2) / Total
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    WindowMetadata,
    fold_window,
)
from bytewax.run import cli_main

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APDEX_T_MS = 500            # Apdex threshold in milliseconds
TOLERATING_LIMIT_MS = 2000  # Upper bound for "tolerating" tier
WINDOW_LENGTH_SECS = 10     # Tumbling window size in seconds

# Align all windows to the Unix epoch (UTC) so window boundaries are consistent
ALIGN_TO = datetime(1970, 1, 1, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Step 1 – Parse each raw JSON line
# ---------------------------------------------------------------------------

def parse_line(line: str) -> tuple[str, dict]:
    """
    Parse a raw JSON line and return a (service, record) keyed pair.
    The Bytewax windowing operators require a (key, value) stream.
    """
    record = json.loads(line)
    # Normalise 'Z' suffix so fromisoformat works on Python < 3.11
    ts_str = record["timestamp"].replace("Z", "+00:00")
    record["_ts"] = datetime.fromisoformat(ts_str)
    return (record["service"], record)


# ---------------------------------------------------------------------------
# Step 2 – Event clock: extract the timestamp embedded in each record
# ---------------------------------------------------------------------------

def get_event_timestamp(record: dict) -> datetime:
    """Return the pre-parsed UTC-aware datetime used by the EventClock."""
    return record["_ts"]


# ---------------------------------------------------------------------------
# Step 3 – Apdex accumulator
# ---------------------------------------------------------------------------

def build_accumulator() -> dict:
    """Return a fresh, empty Apdex accumulator."""
    return {"satisfied": 0, "tolerating": 0, "frustrated": 0}


def fold_apdex(acc: dict, record: dict) -> dict:
    """Classify one response time and add it to the running accumulator."""
    rt = record["response_time_ms"]
    if rt <= APDEX_T_MS:
        acc["satisfied"] += 1
    elif rt <= TOLERATING_LIMIT_MS:
        acc["tolerating"] += 1
    else:
        acc["frustrated"] += 1
    return acc


def merge_accumulators(a: dict, b: dict) -> dict:
    """Combine two accumulators (used when windower merges windows)."""
    return {
        "satisfied": a["satisfied"] + b["satisfied"],
        "tolerating": a["tolerating"] + b["tolerating"],
        "frustrated": a["frustrated"] + b["frustrated"],
    }


# ---------------------------------------------------------------------------
# Step 4 – Re-key with compound (service|window_id) so join can correlate
#           the aggregation result (down) with the window metadata (meta)
# ---------------------------------------------------------------------------

def rekey_with_window_id(item: tuple[str, tuple[int, object]]) -> tuple[str, object]:
    """Rekey from (service, (window_id, payload)) → ('service|window_id', payload)."""
    service, (window_id, payload) = item
    return (f"{service}|{window_id}", payload)


# ---------------------------------------------------------------------------
# Step 5 – Format the joined (acc, meta) pair into a JSON output line
# ---------------------------------------------------------------------------

def format_output(item: tuple[str, tuple[dict, WindowMetadata]]) -> tuple[str, str]:
    """
    Receive a joined item keyed by 'service|window_id' with value (acc, meta).
    Returns a (key, json_string) 2-tuple as required by FileSink.
    """
    compound_key, (acc, meta) = item
    service, _ = compound_key.split("|", 1)

    total = acc["satisfied"] + acc["tolerating"] + acc["frustrated"]
    apdex = (acc["satisfied"] + acc["tolerating"] / 2.0) / total if total > 0 else 0.0

    # Format window open time as ISO-8601 with 'Z' suffix (UTC)
    window_start_iso = meta.open_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    result = {
        "window_start": window_start_iso,
        "service": service,
        "apdex_score": round(apdex, 2),
    }
    # FileSink expects a (key, value) 2-tuple where value is a string
    return (compound_key, json.dumps(result))


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("apdex_flow")

# 1. Read raw lines from input.jsonl
raw = op.input("read_input", flow, FileSource("input.jsonl"))

# 2. Parse each JSON line → (service, record)
keyed = op.map("parse", raw, parse_line)

# 3. EventClock uses the timestamp embedded within each record
clock = EventClock(
    ts_getter=get_event_timestamp,
    # In batch mode all events arrive in order; no late-arrival wait needed
    wait_for_system_duration=timedelta(seconds=0),
)

# 4. 10-second tumbling window aligned to the Unix epoch
windower = TumblingWindower(
    length=timedelta(seconds=WINDOW_LENGTH_SECS),
    align_to=ALIGN_TO,
)

# 5. Fold events per (service × window) into an Apdex accumulator
#    windowed.down → Stream[(service, (window_id, acc))]
#    windowed.meta → Stream[(service, (window_id, WindowMetadata))]
windowed = fold_window(
    "apdex_window",
    keyed,
    clock,
    windower,
    builder=build_accumulator,
    folder=fold_apdex,
    merger=merge_accumulators,
)

# 6. Re-key both streams with a compound 'service|window_id' key so the
#    join operator can correctly pair each result with its window metadata
rekeyed_down = op.map("rekey_down", windowed.down, rekey_with_window_id)
rekeyed_meta = op.map("rekey_meta", windowed.meta, rekey_with_window_id)

# 7. Inner-join: emits once both the accumulator and the metadata have arrived
#    for the same compound key
joined = op.join("join_meta", rekeyed_down, rekeyed_meta)

# 8. Calculate the Apdex score and serialise to a JSON string
#    format_output returns (compound_key, json_string) as FileSink requires
formatted = op.map("format", joined, format_output)

# 9. Write (key, value) pairs to output.jsonl (FileSink writes the value only)
output_path = Path("output.jsonl")
output_path.touch(exist_ok=True)
op.output("write_output", formatted, FileSink(output_path))

# ---------------------------------------------------------------------------
# Entry point – execute with `python apdex_flow.py`
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cli_main(flow)
