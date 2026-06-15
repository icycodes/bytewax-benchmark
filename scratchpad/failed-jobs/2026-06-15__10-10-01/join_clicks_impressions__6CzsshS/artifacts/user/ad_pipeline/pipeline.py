"""Bytewax pipeline to join ad impressions and clicks on ad_id."""

import json

from bytewax import operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow


def parse_json(line: str) -> dict:
    """Parse a JSON string into a Python dict."""
    return json.loads(line)


def cast_clicks_ad_id(record: dict) -> dict:
    """Cast ad_id from string to int in clicks records for type alignment."""
    record["ad_id"] = int(record["ad_id"])
    return record


def key_by_ad_id(record: dict) -> str:
    """Extract ad_id as a string for use as a routing key."""
    return str(record["ad_id"])


def format_joined(joined: tuple) -> tuple:
    """Format a joined tuple (key, (impression, click)) into a (key, json_str) tuple.

    The key is the stringified ad_id, the first element of the tuple
    is the impression record, and the second is the click record.
    """
    key, (impression, click) = joined
    result = {
        "ad_id": int(key),
        "user_id": impression["user_id"],
        "click_time": click["click_time"],
    }
    return (key, json.dumps(result))


flow = Dataflow("ad_join")

# Read impressions
imp_lines = op.input("imp_input", flow, FileSource("impressions.jsonl"))
imp_parsed = op.map("imp_parse", imp_lines, parse_json)
imp_keyed = op.key_on("imp_key", imp_parsed, key_by_ad_id)

# Read clicks
click_lines = op.input("click_input", flow, FileSource("clicks.jsonl"))
click_parsed = op.map("click_parse", click_lines, parse_json)
click_casted = op.map("click_cast", click_parsed, cast_clicks_ad_id)
click_keyed = op.key_on("click_key", click_casted, key_by_ad_id)

# Join on ad_id
joined = op.join("join", imp_keyed, click_keyed)

# Format and output
formatted = op.map("format", joined, format_joined)
op.output("output", formatted, FileSink("joined.jsonl"))
