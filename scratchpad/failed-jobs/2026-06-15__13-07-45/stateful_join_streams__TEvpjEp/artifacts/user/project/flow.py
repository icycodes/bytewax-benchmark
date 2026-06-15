"""Stateful join of event streams using Bytewax.

Joins an impressions stream and a clicks stream on user_id, emitting
a combined record only when both an impression and a click have been
seen for a given user_id.
"""

import json
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

PROJECT_DIR = Path(__file__).parent


def deserialize(line: str) -> dict:
    """Parse a JSON line into a dict."""
    return json.loads(line)


def serialize_value(value: tuple) -> str:
    """Format the joined value into a JSON string.

    The join emits (key, (value1, value2)) where key is user_id,
    value1 is the impression record, and value2 is the click record.
    This function receives only the value part: (impression, click).
    We don't need the key here since it's already the user_id from
    both records.
    """
    impression, click = value
    result = {
        "user_id": impression["user_id"],
        "impression": impression,
        "click": click,
    }
    return json.dumps(result)


flow = Dataflow("join_impressions_clicks")

# ── Input streams ────────────────────────────────────────────────────────
impressions = op.input(
    "impressions_input",
    flow,
    FileSource(PROJECT_DIR / "impressions.jsonl"),
)

clicks = op.input(
    "clicks_input",
    flow,
    FileSource(PROJECT_DIR / "clicks.jsonl"),
)

# ── Deserialize JSON lines ───────────────────────────────────────────────
impressions = op.map("deserialize_impressions", impressions, deserialize)
clicks = op.map("deserialize_clicks", clicks, deserialize)

# ── Key both streams by user_id ──────────────────────────────────────────
keyed_impressions = op.key_on("key_impressions", impressions, lambda e: e["user_id"])
keyed_clicks = op.key_on("key_clicks", clicks, lambda e: e["user_id"])

# ── Stateful inner join (complete) ───────────────────────────────────────
# emit_mode="complete" only emits when both sides have a value for the key.
joined = op.join(
    "join",
    keyed_impressions,
    keyed_clicks,
    emit_mode="complete",
)

# ── Serialize to JSON and write to file ──────────────────────────────────
# Use map_value to keep the stream keyed (required for output routing).
joined_json = op.map_value("serialize", joined, serialize_value)
op.output("joined_output", joined_json, FileSink(PROJECT_DIR / "joined.jsonl"))
