import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("multi_stage_aggregation")

stream = op.input("input", flow, FileSource("input.jsonl"))

def parse_json(line):
    return json.loads(line)

parsed = op.map("parse_json", stream, parse_json)

def key_by_sensor(data):
    return (data["sensor_id"], data["val"])

keyed_stream = op.map("key_by_sensor", parsed, key_by_sensor)

def add_vals(val1, val2):
    return val1 + val2

local_agg = op.reduce_final("local_agg", keyed_stream, add_vals)

def key_by_global(key_val):
    sensor_id, val = key_val
    return ("global", val)

global_keyed = op.map("key_by_global", local_agg, key_by_global)

global_agg = op.reduce_final("global_agg", global_keyed, add_vals)

op.output("out", global_agg, StdOutSink())
