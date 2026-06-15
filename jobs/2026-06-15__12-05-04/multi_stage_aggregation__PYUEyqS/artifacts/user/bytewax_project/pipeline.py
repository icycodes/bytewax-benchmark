import json

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

# Build the dataflow
flow = Dataflow("multi_stage_aggregation")

# --- Input ---
# Read input.jsonl line by line; each line is a raw string.
raw = op.input("input", flow, FileSource("input.jsonl"))

# --- Parse ---
# Each line is a JSON string → parse into a dict, then key by sensor_id.
# Output: (sensor_id, val)
keyed = op.map("parse", raw, lambda line: (
    lambda d: (d["sensor_id"], d["val"])
)(json.loads(line)))

# --- Stage 1: Local aggregation per sensor_id ---
# Sum all values for each sensor; emits once upstream is EOF.
local_sums = op.reduce_final("local_sum", keyed, lambda acc, x: acc + x)

# --- Re-key for global aggregation ---
# Discard the sensor key and use a single "global" key.
global_keyed = op.map("rekey_global", local_sums, lambda kv: ("global", kv[1]))

# --- Stage 2: Global aggregation ---
# Sum all per-sensor totals into one grand total.
grand_total = op.reduce_final("global_sum", global_keyed, lambda acc, x: acc + x)

# --- Output ---
# StdOutSink requires string items, so convert the tuple to its str representation.
string_out = op.map("to_str", grand_total, lambda kv: str(kv))

op.output("output", string_out, StdOutSink())
