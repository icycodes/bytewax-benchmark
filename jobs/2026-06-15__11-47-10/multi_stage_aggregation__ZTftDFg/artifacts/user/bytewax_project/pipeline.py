import json
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

# Define the dataflow
flow = Dataflow("sensor_aggregation")

# Step 1: Read the finite stream of sensor readings from input.jsonl
stream = op.input("input", flow, FileSource("input.jsonl"))

# Step 2: Parse JSON lines and map them to (sensor_id, val) tuples
def parse_and_map(line):
    data = json.loads(line)
    return data["sensor_id"], data["val"]

keyed_stream = op.map("parse_and_map", stream, parse_and_map)

# Step 3: Stage 1 (Local Aggregation) - Group by sensor_id and sum values
local_sum = op.reduce_final("local_sum", keyed_stream, lambda acc, x: acc + x)

# Step 4: Stage 2 (Global Aggregation) - Re-key all items to "global" and sum values
def re_key(item):
    sensor_id, val_sum = item
    return "global", val_sum

global_keyed = op.map("re_key", local_sum, re_key)
global_sum = op.reduce_final("global_sum", global_keyed, lambda acc, x: acc + x)

# Step 5: Output the final grand total to standard output
op.output("out", global_sum, StdOutSink())
