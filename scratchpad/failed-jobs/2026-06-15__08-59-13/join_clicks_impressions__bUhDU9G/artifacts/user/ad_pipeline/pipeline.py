import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

# Top-level functions to avoid lambdas and ensure picklability for recovery
def parse_impression(line: str) -> dict:
    return json.loads(line)

def parse_click(line: str) -> dict:
    return json.loads(line)

def key_impression(item: dict) -> str:
    # ad_id is an integer in impressions, convert to string
    return str(item["ad_id"])

def key_click(item: dict) -> str:
    # ad_id is a string in clicks, convert to string (redundant but safe)
    return str(item["ad_id"])

def format_result(item) -> tuple:
    # item is of the form: (key, (imp, clk))
    key, (imp, clk) = item
    result = {
        "ad_id": imp["ad_id"],  # Keep the integer type from the impressions stream
        "user_id": imp["user_id"],
        "click_time": clk["click_time"]
    }
    return (key, json.dumps(result))

# Initialize the dataflow
flow = Dataflow("join_flow")

# Impressions stream
impressions_source = FileSource(Path("impressions.jsonl"))
impressions_stream = op.input("impressions_input", flow, impressions_source)
parsed_impressions = op.map("parse_impressions", impressions_stream, parse_impression)
keyed_impressions = op.key_on("key_impressions", parsed_impressions, key_impression)

# Clicks stream
clicks_source = FileSource(Path("clicks.jsonl"))
clicks_stream = op.input("clicks_input", flow, clicks_source)
parsed_clicks = op.map("parse_clicks", clicks_stream, parse_click)
keyed_clicks = op.key_on("key_clicks", parsed_clicks, key_click)

# Join the keyed streams
joined_stream = op.join("join_streams", keyed_impressions, keyed_clicks)

# Format the joined result to JSON string
formatted_stream = op.map("format_output", joined_stream, format_result)

# Output to joined.jsonl
joined_sink = FileSink(Path("joined.jsonl"))
op.output("joined_output", formatted_stream, joined_sink)
