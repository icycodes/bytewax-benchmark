import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

flow = Dataflow("join_flow")

# Read streams
impressions_stream = op.input("impressions_input", flow, FileSource("impressions.jsonl"))
clicks_stream = op.input("clicks_input", flow, FileSource("clicks.jsonl"))

# Parse JSON
def parse_json(line):
    return json.loads(line)

impressions = op.map("parse_impressions", impressions_stream, parse_json)
clicks = op.map("parse_clicks", clicks_stream, parse_json)

# Key on user_id
keyed_impressions = op.key_on("key_impressions", impressions, lambda x: x["user_id"])
keyed_clicks = op.key_on("key_clicks", clicks, lambda x: x["user_id"])

# Perform the join
joined = op.join("join", keyed_impressions, keyed_clicks)

# Format to JSON string and return as (key, value) for FileSink
def format_json(key__joined_data):
    key, (imp, click) = key__joined_data
    # Create the combined JSON
    combined = {
        "user_id": key,
        "impression": imp,
        "click": click
    }
    # FileSink expects a tuple (key, string_value)
    return (key, json.dumps(combined))

formatted = op.map("format_json", joined, format_json)

# Write to sink
op.output("out", formatted, FileSink(Path("joined.jsonl")))
