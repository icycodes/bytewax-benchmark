"""
Ad-tech stream join pipeline: joins impression and click events on ad_id.

Reads impressions.jsonl and clicks.jsonl, joins them by ad_id (casting both
to str to resolve the type mismatch), and writes the result to joined.jsonl.

Run with SQLite recovery:
    python -m bytewax.run pipeline:flow -r ./recovery_dir
"""

import json

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op


# ---------------------------------------------------------------------------
# Named (non-lambda) mapper functions — required for picklability so that
# Bytewax's SQLite recovery snapshots can serialise all operator state.
# ---------------------------------------------------------------------------


def parse_json(line: str) -> dict:
    """Parse a raw JSON line into a Python dictionary."""
    return json.loads(line)


def impression_key(record: dict) -> str:
    """Return the routing key for an impression record.

    ad_id is an integer in the impressions stream; cast to str so that
    Bytewax's keyed-stream routing requirement (keys must be str) is met,
    and so the key matches the clicks stream.
    """
    return str(record["ad_id"])


def click_key(record: dict) -> str:
    """Return the routing key for a click record.

    ad_id is already a string in the clicks stream, but we normalise via
    str() for consistency.
    """
    return str(record["ad_id"])


def format_joined(pair: tuple) -> str:
    """Serialise a joined (impression, click) value tuple to a JSON string.

    This is used as a map_value mapper, so the key has already been stripped;
    ``pair`` is just the ``(impression, click)`` inner tuple produced by join.
    The ad_id for the output is taken from the impression record (cast to str
    to match the routing key).
    """
    impression, click = pair
    result = {
        "ad_id": str(impression["ad_id"]),
        "user_id": impression["user_id"],
        "click_time": click["click_time"],
    }
    return json.dumps(result)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("ad-join")

# --- Impressions branch ---
impressions_raw = op.input("impressions_input", flow, FileSource("impressions.jsonl"))
impressions = op.map("parse_impressions", impressions_raw, parse_json)
keyed_impressions = op.key_on("key_impressions", impressions, impression_key)

# --- Clicks branch ---
clicks_raw = op.input("clicks_input", flow, FileSource("clicks.jsonl"))
clicks = op.map("parse_clicks", clicks_raw, parse_json)
keyed_clicks = op.key_on("key_clicks", clicks, click_key)

# --- Inner join on ad_id ---
# emit_mode="complete" emits a tuple only once both sides have arrived for a key.
joined = op.join("join_streams", keyed_impressions, keyed_clicks, emit_mode="complete")

# --- Format and write output ---
# joined is a KeyedStream[(str, (impression_dict, click_dict))].
# map_value keeps the key, transforming only the value — FileSink expects
# a keyed stream of (str, str) tuples.
formatted = op.map_value("format_output", joined, format_joined)
op.output("joined_output", formatted, FileSink("joined.jsonl"))
