"""Bytewax Dynamic Stream Routing.

Reads a JSONL file of events and routes them into four output files
based on the ``type`` field:

- errors.jsonl   – events with type "error"
- metrics.jsonl   – events with type "metric"
- logs.jsonl      – events with type "log"
- dead_letter.jsonl – events with missing or unknown type
"""

import argparse
import json
import os

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main


def parse_json(line: str):
    """Parse a JSON line; return a dict with a sentinel on failure."""
    try:
        obj = json.loads(line)
        if not isinstance(obj, dict):
            # Non-object JSON values go to dead letter
            obj = {"type": None, "_raw": line}
        return obj
    except (json.JSONDecodeError, ValueError):
        return {"type": None, "_raw": line}


def serialize_json(obj: dict) -> str:
    """Serialize a dict back to a JSON string."""
    # If this was a parse-error sentinel, return the raw line instead.
    if "_raw" in obj:
        return obj["_raw"]
    return json.dumps(obj)


def build_flow(input_path: str, output_dir: str) -> Dataflow:
    """Build and return the Bytewax dataflow."""
    flow = Dataflow("route_events")

    # 1. Read lines from the input JSONL file
    lines = op.input("read_input", flow, FileSource(input_path))

    # 2. Parse each line into a Python dict
    events = op.map("parse_json", lines, parse_json)

    # 3. Route events using successive branch operators
    #    First branch: errors vs everything else
    error_branch = op.branch(
        "split_errors", events, lambda e: e.get("type") == "error"
    )
    errors = error_branch.trues
    rest = error_branch.falses

    #    Second branch: metrics vs non-error rest
    metric_branch = op.branch(
        "split_metrics", rest, lambda e: e.get("type") == "metric"
    )
    metrics = metric_branch.trues
    rest2 = metric_branch.falses

    #    Third branch: logs vs dead letter
    log_branch = op.branch(
        "split_logs", rest2, lambda e: e.get("type") == "log"
    )
    logs = log_branch.trues
    dead_letters = log_branch.falses

    # 4. Serialize each branch back to JSON strings
    errors_json = op.map("serialize_errors", errors, serialize_json)
    metrics_json = op.map("serialize_metrics", metrics, serialize_json)
    logs_json = op.map("serialize_logs", logs, serialize_json)
    dead_letters_json = op.map("serialize_dead_letter", dead_letters, serialize_json)

    # 5. Key each stream (required by FileSink for partition routing)
    errors_keyed = op.key_on("key_errors", errors_json, lambda _: "error")
    metrics_keyed = op.key_on("key_metrics", metrics_json, lambda _: "metric")
    logs_keyed = op.key_on("key_logs", logs_json, lambda _: "log")
    dead_letters_keyed = op.key_on(
        "key_dead_letter", dead_letters_json, lambda _: "dead_letter"
    )

    # 6. Write each branch to its own output file
    op.output(
        "write_errors",
        errors_keyed,
        FileSink(os.path.join(output_dir, "errors.jsonl")),
    )
    op.output(
        "write_metrics",
        metrics_keyed,
        FileSink(os.path.join(output_dir, "metrics.jsonl")),
    )
    op.output(
        "write_logs",
        logs_keyed,
        FileSink(os.path.join(output_dir, "logs.jsonl")),
    )
    op.output(
        "write_dead_letter",
        dead_letters_keyed,
        FileSink(os.path.join(output_dir, "dead_letter.jsonl")),
    )

    return flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Route JSON events to category-specific output files."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input JSONL file",
    )
    args = parser.parse_args()

    # Output files go in the project directory (same directory as this script)
    output_dir = os.path.dirname(os.path.abspath(__file__))

    flow = build_flow(args.input, output_dir)
    run_main(flow)


if __name__ == "__main__":
    main()