import json

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("multi_stage_aggregation")

# Read the JSONL file line by line and parse into dicts
lines = op.input("input", flow, FileSource("input.jsonl"))


def parse_line(line: str) -> tuple[str, int]:
    obj = json.loads(line)
    return (obj["sensor_id"], obj["val"])


parsed = op.map("parse", lines, parse_line)

# Stage 1: Local aggregation — sum vals per sensor_id
local_sums = op.fold_final(
    "local_agg",
    parsed,
    lambda: 0,
    lambda acc, val: acc + val,
)

# Stage 2: Re-key to global and compute grand total
global_stream = op.map("rekey", local_sums, lambda kv: ("global", kv[1]))

global_total = op.fold_final(
    "global_agg",
    global_stream,
    lambda: 0,
    lambda acc, val: acc + val,
)

# Output the final result
op.output("output", global_total, StdOutSink())
