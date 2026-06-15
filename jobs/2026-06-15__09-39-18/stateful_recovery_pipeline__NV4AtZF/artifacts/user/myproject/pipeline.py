"""Bytewax sliding window outlier detector with recovery support."""

import json
import math
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import EventClock, SlidingWindower, fold_window
from bytewax import operators as op

# --- Accumulator functions (must be picklable for recovery) ---

def build_acc():
    """Create an empty accumulator: (sum, sum_of_squares, count)."""
    return (0.0, 0.0, 0)


def fold_acc(acc, value):
    """Fold a temperature value into the accumulator.

    acc: (sum, sum_of_squares, count)
    value: dict with 'temp' key
    """
    temp = value["temp"]
    return (acc[0] + temp, acc[1] + temp * temp, acc[2] + 1)


def merge_acc(acc_a, acc_b):
    """Merge two accumulators (for sliding window overlap)."""
    return (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1], acc_a[2] + acc_b[2])


def parse_timestamp(value):
    """Extract timezone-aware datetime from the 'time' field."""
    return datetime.fromisoformat(value["time"].replace("Z", "+00:00"))


# --- Window parameters ---
ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_LENGTH = timedelta(seconds=60)
WINDOW_OFFSET = timedelta(seconds=30)


# --- Build the dataflow ---
flow = Dataflow("sliding_window_outlier")

# 1. Read JSON lines from input file
inp = op.input("read_input", flow, FileSource("input.jsonl"))

# 2. Parse each line from JSON string to dict
parsed = op.map("parse_json", inp, json.loads)

# 3. Key the stream by sensor_id (stateful operators require string keys)
keyed = op.key_on("key_on_sensor", parsed, lambda x: x["sensor_id"])

# 4. Set up event-time clock
clock = EventClock(
    ts_getter=parse_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# 5. Set up sliding window (60s length, 30s offset, aligned to 2026-01-01T00:00:00Z)
windower = SlidingWindower(
    length=WINDOW_LENGTH,
    offset=WINDOW_OFFSET,
    align_to=ALIGN_TO,
)

# 6. Fold window: accumulate (sum, sum_of_squares, count) per window per sensor
windowed = fold_window(
    "fold_temps",
    keyed,
    clock,
    windower,
    builder=build_acc,
    folder=fold_acc,
    merger=merge_acc,
)

# 7. Prepare down stream for joining:
#    Input: (sensor_id, (window_id, (sum, sum_sq, count)))
#    Extract compound key and just the accumulator
def extract_down(item):
    """Extract compound key and accumulator from down stream item."""
    sensor_id, (window_id, acc) = item
    compound_key = f"{sensor_id}_{window_id}"
    return (compound_key, (sensor_id, acc))


down_prepped = op.map("extract_down", windowed.down, extract_down)

# 8. Prepare meta stream for joining:
#    Input: (sensor_id, (window_id, WindowMetadata))
#    Extract compound key and just the metadata
def extract_meta(item):
    """Extract compound key and metadata from meta stream item."""
    sensor_id, (window_id, meta) = item
    compound_key = f"{sensor_id}_{window_id}"
    return (compound_key, (sensor_id, meta))


meta_prepped = op.map("extract_meta", windowed.meta, extract_meta)

# 9. Re-key both streams on compound key for joining
down_keyed = op.key_on("key_down", down_prepped, lambda x: x[0])
meta_keyed = op.key_on("key_meta", meta_prepped, lambda x: x[0])

# 10. Join down and meta streams on compound key
joined = op.join("join_window_meta", down_keyed, meta_keyed)

# 11. Map the joined result to the output format
def format_output(item):
    """Convert joined window data to output JSON string.

    item: (compound_key, ((sensor_id, (sum, sum_sq, count)), (sensor_id, WindowMetadata)))
    """
    compound_key, (down_val, meta_val) = item
    sensor_id, (total, sum_sq, count) = down_val
    _, metadata = meta_val

    if count == 0:
        mean_val = 0.0
        stddev_val = 0.0
    else:
        mean_val = total / count
        variance = (sum_sq / count) - (mean_val * mean_val)
        # Guard against floating-point negative variance
        stddev_val = math.sqrt(max(0.0, variance))

    result = {
        "sensor_id": sensor_id,
        "window_start": metadata.open_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": metadata.close_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mean": mean_val,
        "stddev": stddev_val,
    }
    return json.dumps(result)


formatted = op.map("format_output", joined, format_output)

# 12. Write output to file
op.output("write_output", formatted, FileSink("output.jsonl"))