#!/usr/bin/env python3
"""Per-user Token Bucket rate limiter using Bytewax stateful stream processing."""

import argparse
import json

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, TestingSink, run_main

MAX_CAPACITY = 10.0
REFILL_RATE = 2.0  # tokens per second


def token_bucket_mapper(state, event):
    """Stateful mapper implementing the Token Bucket algorithm.

    Args:
        state: None for the first event of a key, or a tuple
               (current_tokens, last_timestamp).
        event: A dict with keys "user_id", "timestamp", "cost".

    Returns:
        A tuple (new_state, output_event) where output_event is the
        input event dict augmented with an "allowed" key.
    """
    user_id = event["user_id"]
    timestamp = float(event["timestamp"])
    cost = float(event["cost"])

    if state is None:
        # First event for this key: start with full bucket
        current_tokens = MAX_CAPACITY
        last_timestamp = timestamp
    else:
        current_tokens, last_timestamp = state
        # Refill tokens based on elapsed time
        elapsed = timestamp - last_timestamp
        if elapsed > 0:
            current_tokens = min(MAX_CAPACITY, current_tokens + elapsed * REFILL_RATE)
        last_timestamp = timestamp

    # Handle reset signal (cost == -1)
    if cost < 0:
        current_tokens = MAX_CAPACITY
        allowed = True
    elif current_tokens >= cost:
        current_tokens -= cost
        allowed = True
    else:
        # Not enough tokens; do not deduct
        allowed = False

    new_state = (current_tokens, last_timestamp)
    output_event = {
        "user_id": user_id,
        "timestamp": event["timestamp"],
        "cost": event["cost"],
        "allowed": allowed,
    }

    return (new_state, output_event)


def main():
    parser = argparse.ArgumentParser(description="Bytewax Token Bucket Rate Limiter")
    parser.add_argument("--input", required=True, help="Path to input JSONL file")
    parser.add_argument("--output", required=True, help="Path to output JSONL file")
    args = parser.parse_args()

    # Read input events
    with open(args.input, "r") as f:
        events = [json.loads(line.strip()) for line in f if line.strip()]

    # Output collector
    output_list = []

    # Build the dataflow
    flow = Dataflow("rate_limiter")
    inp = op.input("input", flow, TestingSource(events))
    keyed = op.key_on("key_on", inp, key=lambda e: str(e["user_id"]))
    result = op.stateful_map("token_bucket", keyed, token_bucket_mapper)
    unkeyed = op.key_rm("key_rm", result)
    op.output("output", unkeyed, TestingSink(output_list))

    # Execute the dataflow
    run_main(flow)

    # Write output
    with open(args.output, "w") as f:
        for item in output_list:
            f.write(json.dumps(item) + "\n")


if __name__ == "__main__":
    main()