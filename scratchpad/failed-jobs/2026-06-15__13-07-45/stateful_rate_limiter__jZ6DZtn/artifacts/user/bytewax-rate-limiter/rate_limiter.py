#!/usr/bin/env python3
"""Per-user Token Bucket rate limiter using Bytewax stateful stream processing."""

import argparse
import json
from pathlib import Path
from typing import List, Optional

from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource, run_main
import bytewax.operators as op


MAX_CAPACITY = 10.0
REFILL_RATE = 2.0  # tokens per second


class FileSinkPartition(StatelessSinkPartition[str]):
    """A sink partition that appends lines to a file."""

    def __init__(self, output_path: str):
        self._path = output_path
        # Truncate the file on creation
        Path(output_path).write_text("")

    def write_batch(self, items: List[str]) -> None:
        with open(self._path, "a") as f:
            for item in items:
                f.write(item + "\n")

    def close(self) -> None:
        pass


class FileSink(DynamicSink[str]):
    """A dynamic sink that writes to a file."""

    def __init__(self, output_path: str):
        self._output_path = output_path

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> FileSinkPartition:
        return FileSinkPartition(self._output_path)


def build_dataflow(input_path: str, output_path: str) -> Dataflow:
    """Build the Bytewax dataflow for rate limiting."""

    flow = Dataflow("rate_limiter")

    # --- Read input ---
    with open(input_path, "r") as f:
        lines = [line.rstrip("\n") for line in f]

    s = op.input("input", flow, TestingSource(lines))

    # --- Parse JSON ---
    def parse_line(line: str) -> dict:
        return json.loads(line)

    s = op.map("parse_json", s, parse_line)

    # --- Key by user_id (must be string) ---
    def key_on_user(event: dict) -> str:
        return str(event["user_id"])

    s = op.key_on("key_on_user", s, key_on_user)

    # --- Token Bucket stateful map ---
    # State is a tuple: (current_tokens: float, last_timestamp: float)
    # On first event for a key, state is None.
    def token_bucket_mapper(
        state: Optional[tuple], event: dict
    ) -> tuple:
        timestamp = event["timestamp"]
        cost = event["cost"]

        if state is None:
            current_tokens = MAX_CAPACITY
            last_timestamp = timestamp
        else:
            current_tokens, last_timestamp = state

        # Special case: cost of -1 resets the bucket to full
        if cost == -1.0:
            new_state = (MAX_CAPACITY, timestamp)
            event["allowed"] = True
            return (new_state, event)

        # Calculate tokens refilled since last event
        elapsed = timestamp - last_timestamp
        if elapsed < 0:
            # Clock skew or out-of-order event; treat as zero elapsed
            elapsed = 0.0

        refill = elapsed * REFILL_RATE
        current_tokens = min(current_tokens + refill, MAX_CAPACITY)

        # Check if the request can be fulfilled
        if current_tokens >= cost:
            current_tokens -= cost
            allowed = True
        else:
            allowed = False

        new_state = (current_tokens, timestamp)
        event["allowed"] = allowed
        return (new_state, event)

    s = op.stateful_map("token_bucket", s, token_bucket_mapper)

    # --- Drop the key, keep just the event dict ---
    def drop_key(key__event):
        _key, event = key__event
        return event

    s = op.map("drop_key", s, drop_key)

    # --- Serialize to JSON string ---
    def to_json(event: dict) -> str:
        return json.dumps(event)

    s = op.map("to_json", s, to_json)

    # --- Write output ---
    op.output("write_output", s, FileSink(output_path))

    return flow


def main():
    parser = argparse.ArgumentParser(
        description="Per-user Token Bucket rate limiter"
    )
    parser.add_argument("--input", required=True, help="Path to input JSONL file")
    parser.add_argument("--output", required=True, help="Path to output JSONL file")
    args = parser.parse_args()

    flow = build_dataflow(args.input, args.output)
    run_main(flow)


if __name__ == "__main__":
    main()
