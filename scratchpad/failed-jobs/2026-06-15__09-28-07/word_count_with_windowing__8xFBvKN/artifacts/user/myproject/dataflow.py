import sys
import json
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, TumblingWindower, count_window
from bytewax.connectors.files import FileSource, FileSink

if len(sys.argv) != 3:
    print("Usage: python dataflow.py <input_file> <output_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

# Align to 2023-01-01T00:00:00Z
align_to = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
window_length = timedelta(hours=1)

flow = Dataflow("windowed_word_count")

# Read lines from the input file
lines = op.input("read_file", flow, FileSource(input_file))

def parse_json(line):
    # line is a string
    data = json.loads(line)
    # Parse ISO8601 string to datetime
    # We can use datetime.fromisoformat, handle 'Z'
    ts_str = data["time"].replace("Z", "+00:00")
    ts = datetime.fromisoformat(ts_str)
    # Ensure it's UTC aware
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (data["word"], ts)

parsed = op.map("parse_json", lines, parse_json)

def extract_timestamp(item):
    return item[1]

clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = TumblingWindower(
    length=window_length,
    align_to=align_to,
)

# Use count_window to count occurrences
windowed = count_window(
    step_id="count_window",
    up=parsed,
    clock=clock,
    windower=windower,
    key=lambda x: x[0],
)

def format_output(item):
    key, (window_id, count) = item
    window_start = align_to + window_id * window_length
    # Format to ISO8601 string
    window_start_str = window_start.isoformat().replace("+00:00", "Z")
    output_dict = {
        "word": key,
        "window_id": window_start_str,
        "count": count
    }
    return (key, json.dumps(output_dict))

formatted = op.map("format_output", windowed.down, format_output)

# Write to the output file
op.output("write_file", formatted, FileSink(output_file))

if __name__ == "__main__":
    from bytewax.testing import run_main
    run_main(flow)

