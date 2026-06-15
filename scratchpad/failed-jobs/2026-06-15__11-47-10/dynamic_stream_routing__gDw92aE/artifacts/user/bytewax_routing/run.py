import argparse
import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main


def parse_line(line):
    """
    Parse a line of JSONL.
    Returns a dictionary or a special dictionary if invalid.
    """
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        if not isinstance(data, dict):
            # If it's valid JSON but not an object, treat as invalid/dead letter
            return {"_invalid_json": True, "_raw_line": line}
        return data
    except Exception:
        return {"_invalid_json": True, "_raw_line": line}


def is_error(item):
    return item.get("type") == "error"


def is_metric(item):
    return item.get("type") == "metric"


def is_log(item):
    return item.get("type") == "log"


def serialize_item(item):
    return ("", json.dumps(item))


def serialize_dead_letter(item):
    if "_invalid_json" in item and "_raw_line" in item:
        return ("", item["_raw_line"])
    return ("", json.dumps(item))


def main():
    parser = argparse.ArgumentParser(description="Bytewax Dynamic Stream Routing")
    parser.add_argument("--input", required=True, help="Path to the input JSONL file")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Output file paths
    output_files = {
        "errors": Path("errors.jsonl"),
        "metrics": Path("metrics.jsonl"),
        "logs": Path("logs.jsonl"),
        "dead_letter": Path("dead_letter.jsonl"),
    }

    # Pre-create/truncate all output files to ensure they exist and are empty
    # if no events are routed to them.
    for path in output_files.values():
        with open(path, "w") as f:
            pass

    # Build the Bytewax Dataflow
    flow = Dataflow("dynamic_routing")

    # Read the input file line-by-line
    lines = op.input("input_stream", flow, FileSource(input_path))

    # Parse JSON lines, filtering out empty lines
    parsed_stream = op.filter_map("parse_json", lines, parse_line)

    # Route events to multiple streams
    # 1. Branch error vs others
    error_branch = op.branch("is_error", parsed_stream, is_error)
    errors = error_branch.trues
    others1 = error_branch.falses

    # 2. Branch metric vs others
    metric_branch = op.branch("is_metric", others1, is_metric)
    metrics = metric_branch.trues
    others2 = metric_branch.falses

    # 3. Branch log vs others (remaining are dead letters)
    log_branch = op.branch("is_log", others2, is_log)
    logs = log_branch.trues
    dead_letters = log_branch.falses

    # Serialize and output to respective sinks
    op.output(
        "errors_out",
        op.map("serialize_errors", errors, serialize_item),
        FileSink(output_files["errors"]),
    )
    op.output(
        "metrics_out",
        op.map("serialize_metrics", metrics, serialize_item),
        FileSink(output_files["metrics"]),
    )
    op.output(
        "logs_out",
        op.map("serialize_logs", logs, serialize_item),
        FileSink(output_files["logs"]),
    )
    op.output(
        "dead_letter_out",
        op.map("serialize_dead_letter", dead_letters, serialize_dead_letter),
        FileSink(output_files["dead_letter"]),
    )

    # Run the dataflow
    run_main(flow)


if __name__ == "__main__":
    main()
