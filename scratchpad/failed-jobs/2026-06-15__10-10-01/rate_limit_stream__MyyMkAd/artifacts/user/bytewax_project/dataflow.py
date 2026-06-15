"""Token bucket rate limiter dataflow for Bytewax.

Reads JSON events from input.jsonl, applies a per-user token bucket
rate limit, and writes allowed events to output.jsonl.
"""

import json
import pathlib
from typing import Optional, Tuple

from bytewax import operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow

# Token bucket configuration
MAX_CAPACITY = 5.0
INITIAL_TOKENS = 5.0
TOKEN_RATE = 1.0  # tokens per second
COST_PER_EVENT = 1.0

# State type: (current_tokens: float, last_timestamp: float)
TokenBucketState = Tuple[float, float]


def token_bucket_mapper(
    state: Optional[TokenBucketState],
    event: dict,
) -> Tuple[Optional[TokenBucketState], Optional[dict]]:
    """Apply token bucket rate limiting to a single event.

    Args:
        state: Current token bucket state (tokens, last_timestamp) or None if first event.
        event: The incoming JSON event with user_id, event_id, timestamp.

    Returns:
        A tuple of (new_state, result) where result is the event if allowed,
        or None if throttled.
    """
    current_timestamp = event["timestamp"]

    if state is None:
        # First event for this user: initialize bucket
        tokens = INITIAL_TOKENS
    else:
        current_tokens, last_timestamp = state
        elapsed = current_timestamp - last_timestamp
        # Refill tokens based on elapsed time, capped at max capacity
        tokens = min(MAX_CAPACITY, current_tokens + elapsed * TOKEN_RATE)

    if tokens >= COST_PER_EVENT:
        # Allow the event: subtract cost and pass through
        new_tokens = tokens - COST_PER_EVENT
        result = event
    else:
        # Drop the event: no token subtraction
        new_tokens = tokens
        result = None

    new_state: TokenBucketState = (new_tokens, current_timestamp)
    return (new_state, result)


def deserialize(line: str) -> dict:
    """Parse a JSON line into a dict."""
    return json.loads(line)


def serialize_value(item: Tuple[str, dict]) -> Tuple[str, str]:
    """Serialize a dict value to a JSON string, keeping the key."""
    key, value = item
    return (key, json.dumps(value))


def is_not_none(item: Tuple[str, Optional[dict]]) -> bool:
    """Predicate to filter out None values from the stream."""
    _key, value = item
    return value is not None


flow = Dataflow("token_bucket_rate_limiter")

# Read input JSON lines
input_lines = op.input(
    "input",
    flow,
    FileSource(pathlib.Path("/home/user/bytewax_project/input.jsonl")),
)

# Parse JSON
events = op.map("deserialize", input_lines, deserialize)

# Key by user_id (must be a string for Bytewax)
keyed = op.key_on("key_on_user", events, lambda e: str(e["user_id"]))

# Apply token bucket rate limiting per user
limited = op.stateful_map("token_bucket", keyed, token_bucket_mapper)

# Filter out throttled events (where value is None)
allowed = op.filter("filter_allowed", limited, is_not_none)

# Serialize the event value to a JSON string, keeping the key for routing
output_lines = op.map("serialize", allowed, serialize_value)

# Write to output file
op.output(
    "output",
    output_lines,
    FileSink(pathlib.Path("/home/user/bytewax_project/output.jsonl")),
)
