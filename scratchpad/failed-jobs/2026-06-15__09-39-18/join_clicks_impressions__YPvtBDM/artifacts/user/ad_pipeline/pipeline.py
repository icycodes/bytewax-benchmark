"""Bytewax dataflow that joins ad impressions and clicks on ad_id."""

import json

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op


def parse_impression(line):
    """Parse an impression JSON line.

    Converts ad_id from int to str so it matches the clicks stream
    and is a valid Bytewax routing key (must be a string).
    """
    data = json.loads(line)
    data["ad_id"] = str(data["ad_id"])
    return data


def parse_click(line):
    """Parse a click JSON line. ad_id is already a string."""
    return json.loads(line)


def get_ad_id(record):
    """Extract ad_id from a parsed record to use as the join key."""
    return record["ad_id"]


def format_joined(item):
    """Format a joined record as a (key, json_string) tuple for output.

    The output operator requires (key, value) 2-tuples for routing.
    ad_id is converted back to int so the output matches the original
    impressions type.
    """
    key, (impression, click) = item
    return (
        key,
        json.dumps({
            "ad_id": int(impression["ad_id"]),
            "user_id": impression["user_id"],
            "click_time": click["click_time"],
        }),
    )


flow = Dataflow("ad_join")

# Read input streams from JSONL files
impressions = op.input(
    "imp_input", flow, FileSource("/home/user/ad_pipeline/impressions.jsonl")
)
clicks = op.input(
    "click_input", flow, FileSource("/home/user/ad_pipeline/clicks.jsonl")
)

# Parse JSON lines into dicts
impressions_parsed = op.map("parse_imp", impressions, parse_impression)
clicks_parsed = op.map("parse_click", clicks, parse_click)

# Convert to keyed streams (routing keys must be strings)
impressions_keyed = op.key_on("key_imp", impressions_parsed, get_ad_id)
clicks_keyed = op.key_on("key_click", clicks_parsed, get_ad_id)

# Inner join on ad_id
joined = op.join("join_on_ad_id", impressions_keyed, clicks_keyed)

# Format output as JSON strings
joined_formatted = op.map("format_output", joined, format_joined)

# Write to output file
op.output(
    "out", joined_formatted, FileSink("/home/user/ad_pipeline/joined.jsonl")
)
