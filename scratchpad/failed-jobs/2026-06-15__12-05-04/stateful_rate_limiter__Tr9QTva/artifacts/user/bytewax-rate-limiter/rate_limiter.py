"""
Per-user Token Bucket rate limiter built with Bytewax stateful stream processing.

Token Bucket parameters:
  - Max capacity : 10.0 tokens
  - Refill rate  : 2.0 tokens per second
  - Initial state: full bucket (10.0 tokens)

A cost of -1 is a special "reset" signal that always succeeds and
restores the bucket to full capacity.

Usage:
    python rate_limiter.py --input events.jsonl --output results.jsonl
"""

import argparse
import json
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# ---------------------------------------------------------------------------
# Token Bucket constants
# ---------------------------------------------------------------------------
MAX_TOKENS: float = 10.0
REFILL_RATE: float = 2.0   # tokens per second
RESET_COST: float = -1.0


# ---------------------------------------------------------------------------
# Sink: write (key, result_dict) pairs to a JSONL file
# ---------------------------------------------------------------------------
class _FileSinkPartition(StatelessSinkPartition):
    """One file-handle per worker (only worker 0 runs in run_main)."""

    def __init__(self, path: str) -> None:
        # Open in write mode; run_main is single-worker so no race condition.
        self._fh = open(path, "w", encoding="utf-8")

    def write_batch(self, items):
        for _key, record in items:
            self._fh.write(json.dumps(record) + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()


class FileSink(DynamicSink):
    """Append JSON lines to *path*, one record per stream item."""

    def __init__(self, path: str) -> None:
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _FileSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Token Bucket mapper  (used with op.stateful_map)
# ---------------------------------------------------------------------------
# State type: Optional[Tuple[float, float]]  →  (current_tokens, last_timestamp)
# Value type: dict  →  the parsed event {"user_id", "timestamp", "cost"}
# Emit type : dict  →  the event augmented with {"allowed": bool}

def token_bucket_mapper(
    state: Optional[Tuple[float, float]],
    event: dict,
) -> Tuple[Optional[Tuple[float, float]], dict]:
    """
    Bytewax stateful_map callback implementing a Token Bucket per user.

    Parameters
    ----------
    state:
        ``None`` on the very first event for a key; afterwards a
        ``(current_tokens, last_timestamp)`` tuple.
    event:
        Parsed input record with keys ``user_id``, ``timestamp``, ``cost``.

    Returns
    -------
    (new_state, output_record)
    """
    timestamp: float = event["timestamp"]
    cost: float = event["cost"]

    # ---- Initialise state on first encounter --------------------------------
    if state is None:
        current_tokens: float = MAX_TOKENS
        last_timestamp: float = timestamp
    else:
        current_tokens, last_timestamp = state

    # ---- Refill tokens based on elapsed time --------------------------------
    elapsed: float = max(0.0, timestamp - last_timestamp)
    current_tokens = min(MAX_TOKENS, current_tokens + elapsed * REFILL_RATE)

    # ---- Evaluate the request -----------------------------------------------
    if cost == RESET_COST:
        # Special reset signal: restore full bucket, always allowed
        new_tokens = MAX_TOKENS
        allowed = True
    elif current_tokens >= cost:
        new_tokens = current_tokens - cost
        allowed = True
    else:
        # Not enough tokens – bucket unchanged
        new_tokens = current_tokens
        allowed = False

    new_state: Tuple[float, float] = (new_tokens, timestamp)
    output_record = {**event, "allowed": allowed}
    return new_state, output_record


# ---------------------------------------------------------------------------
# Dataflow builder
# ---------------------------------------------------------------------------

def build_dataflow(input_path: str, output_path: str) -> Dataflow:
    """Construct and return the Bytewax Dataflow."""

    # Read all JSONL lines eagerly so TestingSource can iterate them.
    with open(input_path, "r", encoding="utf-8") as fh:
        raw_lines = [line.rstrip("\n") for line in fh if line.strip()]

    flow = Dataflow("rate_limiter")

    # 1. Ingest raw JSON strings
    stream = op.input("input", flow, TestingSource(raw_lines))

    # 2. Parse each line into an event dict
    stream = op.map("parse_json", stream, json.loads)

    # 3. Key by user_id (must be a plain str for Bytewax routing)
    keyed = op.key_on("key_by_user", stream, lambda event: str(event["user_id"]))

    # 4. Apply per-user token bucket via stateful_map
    rated = op.stateful_map("token_bucket", keyed, token_bucket_mapper)

    # 5. Write results to the output JSONL file
    op.output("output", rated, FileSink(output_path))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Per-user token bucket rate limiter using Bytewax."
    )
    parser.add_argument(
        "--input",
        required=True,
        metavar="FILE",
        help="Path to the input JSONL file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="FILE",
        help="Path to the output JSONL file.",
    )
    args = parser.parse_args()

    flow = build_dataflow(args.input, args.output)
    run_main(flow)


if __name__ == "__main__":
    main()
