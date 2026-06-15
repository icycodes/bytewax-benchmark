import json
from pathlib import Path

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink

import bytewax.operators as op

MAX_TOKENS = 5.0
REFILL_RATE = 1.0  # tokens per second

INPUT_PATH = "/home/user/bytewax_project/input.jsonl"
OUTPUT_PATH = Path("/home/user/bytewax_project/output.jsonl")


def token_bucket(state, event):
    """Token bucket rate limiter per user.

    State is None (first event) or a tuple (current_tokens, last_timestamp).
    Returns (new_state, event_dict) if allowed, or (new_state, None) if dropped.
    """
    event_ts = event["timestamp"]

    if state is None:
        # First event for this user: start with MAX_TOKENS, no elapsed time
        current_tokens = MAX_TOKENS
    else:
        current_tokens, last_ts = state
        elapsed = event_ts - last_ts
        current_tokens = min(current_tokens + elapsed * REFILL_RATE, MAX_TOKENS)

    if current_tokens >= 1.0:
        current_tokens -= 1.0
        result = event
    else:
        result = None

    # Always update last timestamp to current event's timestamp
    new_state = (current_tokens, event_ts)
    return (new_state, result)


flow = Dataflow("rate_limiter")

# Read input JSON lines
inp = op.input("inp", flow, FileSource(INPUT_PATH))

# Parse each line from JSON string to dict
parsed = op.map("parse", inp, json.loads)

# Key by user_id (must be string for stateful operations)
keyed = op.key_on("key_on_user", parsed, lambda event: str(event["user_id"]))

# Apply token bucket rate limiting
limited = op.stateful_map("token_bucket", keyed, token_bucket)

# Drop throttled events (those where token_bucket returned None)
filtered = op.filter_value("filter_allowed", limited, lambda event: event is not None)

# Serialize allowed event dicts to JSON strings (keeping key for sink routing)
output_kv = op.map_value("serialize", filtered, lambda event: json.dumps(event))

# Write to output file
op.output("out", output_kv, FileSink(OUTPUT_PATH))