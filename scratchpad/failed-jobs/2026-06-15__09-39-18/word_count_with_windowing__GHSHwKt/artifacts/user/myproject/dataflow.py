"""Bytewax windowed word count dataflow.

Reads a JSONL input file containing events with a word and timestamp,
counts word occurrences within 1-hour tumbling windows based on event time,
and writes the results to a JSONL output file.

Usage:
    python dataflow.py <input_file> <output_file>
"""

import json
import sys
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, TumblingWindower, count_window
from bytewax.testing import run_main

# Window configuration
ALIGN_TO = datetime(2023, 1, 1, tzinfo=timezone.utc)
WINDOW_LENGTH = timedelta(hours=1)


def parse_line(line):
    """Parse a JSONL line into a (word, datetime) tuple."""
    data = json.loads(line)
    word = data["word"]
    # Parse ISO8601 timestamp; handle both "Z" suffix and "+00:00"
    time_str = data["time"]
    dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    return (word, dt)


def window_id_to_start(window_id):
    """Convert a window ID integer to the ISO8601 start time string."""
    window_start = ALIGN_TO + WINDOW_LENGTH * window_id
    return window_start.isoformat()


def format_output(item):
    """Format (key, (window_id, count)) into a (key, JSONL string) tuple."""
    key, (window_id, count) = item
    result = {
        "word": key,
        "window_id": window_id_to_start(window_id),
        "count": count,
    }
    return (key, json.dumps(result))


def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    flow = Dataflow("windowed_word_count")

    # Read input JSONL file line by line
    inp = op.input("read_input", flow, FileSource(input_file))

    # Parse each JSONL line into (word, datetime)
    parsed = op.map("parse_line", inp, parse_line)

    # Define event-time clock using the timestamp from each event
    clock = EventClock(
        ts_getter=lambda x: x[1],
        wait_for_system_duration=timedelta(seconds=0),
    )

    # Define 1-hour tumbling windows aligned to 2023-01-01T00:00:00Z
    windower = TumblingWindower(
        length=WINDOW_LENGTH,
        align_to=ALIGN_TO,
    )

    # Count occurrences of each word within each window
    windowed = count_window(
        step_id="count_window",
        up=parsed,
        clock=clock,
        windower=windower,
        key=lambda x: x[0],
    )

    # Format the output as JSONL
    formatted = op.map("format_output", windowed.down, format_output)

    # Write to output JSONL file
    op.output("write_output", formatted, FileSink(output_file))

    run_main(flow)


if __name__ == "__main__":
    main()