"""
Bytewax dataflow: per-user token-bucket rate limiter.

Token-bucket parameters
-----------------------
* Max capacity  : 5.0 tokens
* Initial tokens: 5.0 tokens
* Refill rate   : 1.0 token / second  (elapsed_seconds * 1.0)
* Cost per event: 1.0 token

State stored as a plain tuple ``(current_tokens: float, last_timestamp: float)``
so that it is fully picklable for Bytewax's SQLite recovery mechanism.
"""

import json
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_TOKENS: float = 5.0
INITIAL_TOKENS: float = 5.0
REFILL_RATE: float = 1.0   # tokens per second
TOKEN_COST: float = 1.0

# Picklable state type alias: (current_tokens, last_timestamp)
BucketState = Tuple[float, float]


# ---------------------------------------------------------------------------
# Token-bucket logic
# ---------------------------------------------------------------------------

def token_bucket_mapper(
    state: Optional[BucketState],
    event: dict,
) -> Tuple[Optional[BucketState], Optional[dict]]:
    """
    Stateful mapper that applies the token-bucket algorithm.

    Parameters
    ----------
    state:
        ``None`` on the very first event for a user, otherwise a tuple
        ``(current_tokens, last_timestamp)``.
    event:
        Parsed JSON event dict containing at least ``timestamp``.

    Returns
    -------
    new_state:
        Updated ``(current_tokens, last_timestamp)`` tuple.
    output:
        The original *event* dict if allowed, ``None`` if throttled.
    """
    current_ts: float = event["timestamp"]

    if state is None:
        # First event for this user – bucket starts full.
        current_tokens: float = INITIAL_TOKENS
        last_ts: float = current_ts
    else:
        current_tokens, last_ts = state
        elapsed: float = max(0.0, current_ts - last_ts)
        # Refill and cap at MAX_TOKENS
        current_tokens = min(MAX_TOKENS, current_tokens + elapsed * REFILL_RATE)

    # Decide allow / drop
    if current_tokens >= TOKEN_COST:
        current_tokens -= TOKEN_COST
        output: Optional[dict] = event
    else:
        output = None  # throttled – will be filtered out downstream

    # Always advance the timestamp regardless of allow / drop
    new_state: BucketState = (current_tokens, current_ts)
    return new_state, output


# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------

flow = Dataflow("rate_limiter")

# 1. Read raw JSON lines from the input file.
raw_stream = op.input("file_in", flow, FileSource("/home/user/bytewax_project/input.jsonl"))

# 2. Parse each line into a dict.
parsed = op.map("parse_json", raw_stream, json.loads)

# 3. Key the stream by user_id (must be a string for stateful_map).
keyed = op.key_on("key_by_user", parsed, lambda event: str(event["user_id"]))

# 4. Apply the token-bucket rate limiter (state is per user_id key).
rate_limited = op.stateful_map("token_bucket", keyed, token_bucket_mapper)

# 5. Remove throttled events (None values).
allowed = op.filter("drop_throttled", rate_limited, lambda kv: kv[1] is not None)

# 6. Serialize the event value to a JSON string, keeping the (key, value) tuple
#    that FileSink (FixedPartitionedSink) requires for routing.
serialized = op.map("to_json_str", allowed, lambda kv: (kv[0], json.dumps(kv[1])))

# 7. Write allowed events to the output file.
op.output("file_out", serialized, FileSink(Path("/home/user/bytewax_project/output.jsonl")))
