"""Stateful stream join of impressions and clicks by user_id using Bytewax."""

import json
from pathlib import Path

from bytewax import operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).parent
IMPRESSIONS_FILE = PROJECT_DIR / "impressions.jsonl"
CLICKS_FILE = PROJECT_DIR / "clicks.jsonl"
OUTPUT_FILE = PROJECT_DIR / "joined.jsonl"

# Ensure the output file exists (FileSink requires it to be present).
OUTPUT_FILE.touch()

# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------
flow = Dataflow("impression_click_join")

# --- Impressions stream ---
# FileSource emits raw str lines.
impressions_raw = op.input("impressions_in", flow, FileSource(IMPRESSIONS_FILE))

# Key each line by user_id → stream of (user_id, raw_line)
impressions_keyed = op.key_on(
    "key_impressions",
    impressions_raw,
    lambda line: json.loads(line)["user_id"],
)

# Replace the raw line value with the parsed dict.
impressions = op.map(
    "parse_impressions",
    impressions_keyed,
    lambda kv: (kv[0], json.loads(kv[1])),
)

# --- Clicks stream ---
clicks_raw = op.input("clicks_in", flow, FileSource(CLICKS_FILE))

clicks_keyed = op.key_on(
    "key_clicks",
    clicks_raw,
    lambda line: json.loads(line)["user_id"],
)

clicks = op.map(
    "parse_clicks",
    clicks_keyed,
    lambda kv: (kv[0], json.loads(kv[1])),
)

# ---------------------------------------------------------------------------
# Stateful inner join
#
# emit_mode="complete" → emit only when BOTH sides carry a value (inner join).
# insert_mode="last"   → keep the most-recent event per side per key.
#
# Output shape: (user_id, (impression_dict, click_dict))
# ---------------------------------------------------------------------------
joined = op.join(
    "join_streams",
    impressions,
    clicks,
    insert_mode="last",
    emit_mode="complete",
)

# ---------------------------------------------------------------------------
# Format: (user_id, (impression, click)) → (user_id, json_string)
#
# FileSink is a FixedPartitionedSink whose part_fn receives the *key* from a
# (key, value) tuple, so the stream must remain keyed after formatting.
# ---------------------------------------------------------------------------
def format_joined(kv):
    """Convert a joined record into a (key, JSON-string) tuple."""
    user_id, (impression, click) = kv
    record = {
        "user_id": user_id,
        "impression": impression,
        "click": click,
    }
    return (user_id, json.dumps(record))


formatted = op.map("format_output", joined, format_joined)

# ---------------------------------------------------------------------------
# Sink — write one JSON object per line to joined.jsonl
# ---------------------------------------------------------------------------
op.output("joined_out", formatted, FileSink(OUTPUT_FILE))
