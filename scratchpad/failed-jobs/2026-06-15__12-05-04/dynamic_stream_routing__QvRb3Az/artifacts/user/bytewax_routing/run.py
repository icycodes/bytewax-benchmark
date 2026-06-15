"""
Bytewax Dynamic Stream Routing
-------------------------------
Reads a JSONL file and routes each event to one of four output files
based on the value of the ``type`` field:

  * errors.jsonl      – events where type == "error"
  * metrics.jsonl     – events where type == "metric"
  * logs.jsonl        – events where type == "log"
  * dead_letter.jsonl – events missing the type field or with an unknown type

Usage
-----
    python run.py --input <input_file>

Bytewax version: 0.21.x
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.run import cli_main

# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bytewax dynamic stream router – routes JSONL events by type."
    )
    parser.add_argument(
        "--input",
        required=True,
        metavar="INPUT_FILE",
        help="Path to the input JSONL file.",
    )
    # cli_main also accepts additional arguments; parse only known args so that
    # Bytewax's own runner arguments (e.g. -w, -p) are not consumed here.
    args, _ = parser.parse_known_args()
    return args


# ---------------------------------------------------------------------------
# Custom sink: appends JSON lines to a file
# ---------------------------------------------------------------------------

class _JsonlSinkPartition(StatelessSinkPartition):
    """Writes string items as lines to an output file."""

    def __init__(self, path: Path) -> None:
        # Open in append mode so that partial writes during multi-worker runs
        # do not overwrite each other. For a single-worker, batch run we
        # truncate the file first (see JsonlFileSink.__init__).
        self._fh = open(path, "a", encoding="utf-8")

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._fh.write(item)
            if not item.endswith("\n"):
                self._fh.write("\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class JsonlFileSink(DynamicSink):
    """A :class:`~bytewax.outputs.DynamicSink` that writes JSONL to *path*.

    The output file is truncated when the sink is instantiated so that
    each run of the dataflow produces a clean output.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        # Truncate / create the file before the dataflow starts writing.
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text("", encoding="utf-8")

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _JsonlSinkPartition:
        return _JsonlSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Event parsing helper
# ---------------------------------------------------------------------------

def _parse_json_line(line: str) -> dict:
    """Parse a JSON line; on failure return a sentinel dead-letter dict."""
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        # Wrap malformed lines in a dead-letter envelope so they are not lost.
        return {"_parse_error": str(exc), "_raw": line}


def _to_json_str(event: dict) -> str:
    """Serialize *event* back to a compact JSON string (no trailing newline)."""
    return json.dumps(event, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Routing predicates
# ---------------------------------------------------------------------------

def _is_error(event: dict) -> bool:
    return event.get("type") == "error"


def _is_metric(event: dict) -> bool:
    return event.get("type") == "metric"


def _is_log(event: dict) -> bool:
    return event.get("type") == "log"


def _is_dead_letter(event: dict) -> bool:
    """Catch-all: events that do not match any known type."""
    return event.get("type") not in ("error", "metric", "log")


# ---------------------------------------------------------------------------
# Dataflow builder
# ---------------------------------------------------------------------------

def build_dataflow(input_path: Path, output_dir: Path) -> Dataflow:
    """Construct and return the Bytewax :class:`~bytewax.dataflow.Dataflow`.

    Parameters
    ----------
    input_path:
        Absolute or relative path to the input JSONL file.
    output_dir:
        Directory where the four output JSONL files will be written.

    Returns
    -------
    Dataflow
        A fully wired Bytewax dataflow ready to be executed.
    """
    flow = Dataflow("event_router")

    # ------------------------------------------------------------------
    # 1. Ingest – read raw lines from the JSONL file
    # ------------------------------------------------------------------
    raw_lines: op.Stream = op.input(
        "read_jsonl",
        flow,
        FileSource(input_path),
    )

    # ------------------------------------------------------------------
    # 2. Parse – convert each raw string line into a Python dict
    # ------------------------------------------------------------------
    events: op.Stream = op.map("parse_json", raw_lines, _parse_json_line)

    # ------------------------------------------------------------------
    # 3. Route – use cascaded `branch` to split by event type
    #
    #   branch() returns BranchOut(trues, falses).
    #   We peel off one category at a time; the `falses` stream carries
    #   everything that did not match and flows into the next branch.
    # ------------------------------------------------------------------

    # Branch 1 – separate "error" events
    b_error = op.branch("route_error", events, _is_error)
    error_stream = b_error.trues        # type == "error"
    remainder1   = b_error.falses       # everything else

    # Branch 2 – separate "metric" events from the remainder
    b_metric = op.branch("route_metric", remainder1, _is_metric)
    metric_stream = b_metric.trues      # type == "metric"
    remainder2    = b_metric.falses     # everything else

    # Branch 3 – separate "log" events from the remainder
    b_log = op.branch("route_log", remainder2, _is_log)
    log_stream       = b_log.trues      # type == "log"
    dead_letter_stream = b_log.falses   # unknown / missing type → dead letter

    # ------------------------------------------------------------------
    # 4. Serialize – turn dicts back into JSON strings before writing
    # ------------------------------------------------------------------
    error_json       = op.map("serialize_error",       error_stream,       _to_json_str)
    metric_json      = op.map("serialize_metric",      metric_stream,      _to_json_str)
    log_json         = op.map("serialize_log",         log_stream,         _to_json_str)
    dead_letter_json = op.map("serialize_dead_letter", dead_letter_stream, _to_json_str)

    # ------------------------------------------------------------------
    # 5. Sink – write each branch to its own JSONL file
    # ------------------------------------------------------------------
    op.output("sink_errors",      error_json,       JsonlFileSink(output_dir / "errors.jsonl"))
    op.output("sink_metrics",     metric_json,      JsonlFileSink(output_dir / "metrics.jsonl"))
    op.output("sink_logs",        log_json,         JsonlFileSink(output_dir / "logs.jsonl"))
    op.output("sink_dead_letter", dead_letter_json, JsonlFileSink(output_dir / "dead_letter.jsonl"))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = _parse_args()

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Output files are written into the same directory as this script.
    output_dir = Path(__file__).parent.resolve()

    flow = build_dataflow(input_path, output_dir)

    # cli_main runs the dataflow locally (single process, single worker by
    # default). Pass -w N on the command line to use multiple workers.
    cli_main(flow)
