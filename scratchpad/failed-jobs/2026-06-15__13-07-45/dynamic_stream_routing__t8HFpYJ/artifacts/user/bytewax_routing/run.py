#!/usr/bin/env python
"""Bytewax dataflow that reads JSONL events and dynamically routes them
to output files based on the `type` field.

Usage:
    python run.py --input <input_file>
"""

import argparse
import json
import pathlib
import sys

from bytewax import operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow

# ── Constants ────────────────────────────────────────────────────────────────

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent

OUTPUTS = {
    "error": OUTPUT_DIR / "errors.jsonl",
    "metric": OUTPUT_DIR / "metrics.jsonl",
    "log": OUTPUT_DIR / "logs.jsonl",
    "dead_letter": OUTPUT_DIR / "dead_letter.jsonl",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_json(line: str) -> dict:
    """Parse a line of JSON into a Python dict."""
    return json.loads(line)


def classify_event(event: dict) -> str:
    """Return the routing category for an event.

    Returns one of: "error", "metric", "log", or "dead_letter".
    """
    event_type = event.get("type")
    if event_type in OUTPUTS:
        return event_type
    return "dead_letter"


def serialize(event: dict) -> str:
    """Serialize a dict back to a JSON string."""
    return json.dumps(event, ensure_ascii=False)


# ── Dataflow ─────────────────────────────────────────────────────────────────

def build_dataflow(input_path: pathlib.Path) -> Dataflow:
    """Construct the Bytewax dataflow for routing events."""

    flow = Dataflow("jsonl_router")

    # 1. Read lines from the input JSONL file.
    lines = op.input("read_input", flow, FileSource(input_path))

    # 2. Parse JSON strings into dicts.
    events = op.map("parse_json", lines, parse_json)

    # 3. Route: error vs non-error
    branch_error = op.branch("route_error", events, lambda e: e.get("type") == "error")
    errors = branch_error.trues
    non_errors = branch_error.falses

    # 4. Route: metric vs non-metric (from non-errors)
    branch_metric = op.branch(
        "route_metric", non_errors, lambda e: e.get("type") == "metric"
    )
    metrics = branch_metric.trues
    non_metrics = branch_metric.falses

    # 5. Route: log vs dead-letter (from non-metrics)
    branch_log = op.branch(
        "route_log", non_metrics, lambda e: e.get("type") == "log"
    )
    logs = branch_log.trues
    dead_letters = branch_log.falses

    # 6. Serialize each category back to JSON strings.
    errors_json = op.map("serialize_errors", errors, serialize)
    metrics_json = op.map("serialize_metrics", metrics, serialize)
    logs_json = op.map("serialize_logs", logs, serialize)
    dead_json = op.map("serialize_dead", dead_letters, serialize)

    # 7. Wrap each item as a (key, value) tuple for FixedPartitionedSink.
    errors_kv = op.map("key_errors", errors_json, lambda s: ("error", s))
    metrics_kv = op.map("key_metrics", metrics_json, lambda s: ("metric", s))
    logs_kv = op.map("key_logs", logs_json, lambda s: ("log", s))
    dead_kv = op.map("key_dead", dead_json, lambda s: ("dead_letter", s))

    # 8. Write to output files.
    op.output("write_errors", errors_kv, FileSink(OUTPUTS["error"]))
    op.output("write_metrics", metrics_kv, FileSink(OUTPUTS["metric"]))
    op.output("write_logs", logs_kv, FileSink(OUTPUTS["log"]))
    op.output("write_dead_letter", dead_kv, FileSink(OUTPUTS["dead_letter"]))

    return flow


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Route JSONL events by type into separate output files."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=pathlib.Path,
        help="Path to the input JSONL file.",
    )
    args = parser.parse_args()

    input_path = args.input.resolve()
    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    flow = build_dataflow(input_path)

    # Use bytewax's built-in CLI entry point for running the dataflow.
    from bytewax.run import cli_main

    cli_main(flow)


if __name__ == "__main__":
    main()
