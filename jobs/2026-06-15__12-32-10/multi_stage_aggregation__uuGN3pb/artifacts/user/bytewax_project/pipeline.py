import json

import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("multi_stage_agg")

# Read input from JSONL file
inp = op.input("read", flow, FileSource("input.jsonl"))

# Parse each JSON line into a dict
parsed = op.map("parse", inp, json.loads)

# Key the stream by sensor_id
keyed = op.key_on("key_sensor", parsed, lambda x: x["sensor_id"])

# Extract just the numeric value from each dict
vals = op.map_value("extract_val", keyed, lambda x: x["val"])

# Stage 1: Local aggregation — sum values per sensor
sensor_sums = op.reduce_final("sensor_sum", vals, lambda acc, x: acc + x)

# Re-key all items to a single global key for the next stage
global_keyed = op.map("rekey_global", sensor_sums, lambda x: ("global", x[1]))

# Stage 2: Global aggregation — grand total of all sensor sums
global_total = op.reduce_final("global_sum", global_keyed, lambda acc, x: acc + x)

# Output the final result to stdout
op.output("out", global_total, StdOutSink())