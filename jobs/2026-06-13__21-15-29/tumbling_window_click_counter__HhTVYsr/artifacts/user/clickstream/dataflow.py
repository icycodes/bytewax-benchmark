from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators.windowing import EventClock, TumblingWindower, count_window

# Define the dataflow
flow = Dataflow("click_counter")

# Input source: reads click events from input.jsonl line-by-line
inp = op.input("inp", flow, FileSource("/home/user/clickstream/input.jsonl"))

# Parse each JSON line
def parse_line(line: str):
    return json.loads(line)

parsed = op.map("parse_line", inp, parse_line)

# Clock and Windower for event-time tumbling windows
align_to = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
clock = EventClock(
    ts_getter=lambda event: datetime.fromisoformat(event["timestamp"]),
    wait_for_system_duration=timedelta(seconds=0)
)
windower = TumblingWindower(
    length=timedelta(minutes=5),
    align_to=align_to
)

# Windowed count of clicks per user in each 5-minute tumbling window
windowed = count_window(
    step_id="count_clicks",
    up=parsed,
    clock=clock,
    windower=windower,
    key=lambda event: str(event["user_id"])
)

# Format the windowed counts to the required output JSON format
def format_output(item):
    user_id, (window_id, count) = item
    window_start = align_to + window_id * timedelta(minutes=5)
    out_obj = {
        "user_id": str(user_id),
        "window_start": window_start.isoformat(),
        "count": count
    }
    return (str(user_id), json.dumps(out_obj))

formatted = op.map("format_output", windowed.down, format_output)

# Output sink: writes formatted JSON lines to output.jsonl
op.output("out", formatted, FileSink(Path("/home/user/clickstream/output.jsonl")))
