import json
import argparse
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def parse_event(line):
    try:
        return json.loads(line)
    except Exception:
        return {"__raw__": line}

def serialize_event(event):
    if "__raw__" in event:
        return ("dead_letter", event["__raw__"])
    return (event.get("type", "dead_letter"), json.dumps(event))

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    flow = Dataflow("routing")
    lines = op.input("inp", flow, FileSource(Path(args.input)))
    events = op.map("parse", lines, parse_event)

    # Route: error
    b_error = op.branch("is_error", events, lambda e: e.get("type") == "error")
    errors = b_error.trues
    not_errors = b_error.falses

    # Route: metric
    b_metric = op.branch("is_metric", not_errors, lambda e: e.get("type") == "metric")
    metrics = b_metric.trues
    not_metric = b_metric.falses

    # Route: log
    b_log = op.branch("is_log", not_metric, lambda e: e.get("type") == "log")
    logs = b_log.trues
    dead_letter = b_log.falses

    # Serialize
    errors_out = op.map("ser_error", errors, serialize_event)
    metrics_out = op.map("ser_metric", metrics, serialize_event)
    logs_out = op.map("ser_log", logs, serialize_event)
    dead_out = op.map("ser_dead", dead_letter, serialize_event)

    # Sinks
    op.output("out_error", errors_out, FileSink(Path("errors.jsonl")))
    op.output("out_metric", metrics_out, FileSink(Path("metrics.jsonl")))
    op.output("out_log", logs_out, FileSink(Path("logs.jsonl")))
    op.output("out_dead", dead_out, FileSink(Path("dead_letter.jsonl")))

    run_main(flow)

if __name__ == "__main__":
    run()
