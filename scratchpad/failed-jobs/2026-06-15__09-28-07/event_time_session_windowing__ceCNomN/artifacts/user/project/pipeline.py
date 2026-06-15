import json
from datetime import datetime, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource, FileSink

flow = Dataflow("session_aggregator")

stream = op.input("input", flow, FileSource("input.jsonl"))

def parse_json(line):
    return json.loads(line)

stream = op.map("parse_json", stream, parse_json)

def key_by_user(data):
    return (str(data["user_id"]), data)

keyed_stream = op.map("key_by_user", stream, key_by_user)

def extract_time(data):
    # data is the value part of the keyed stream
    return datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

clock = win.EventClock(extract_time, wait_for_system_duration=timedelta(seconds=0))

windower = win.SessionWindower(gap=timedelta(seconds=10))

def builder():
    return 0

def folder(count, data):
    return count + 1

def merger(count1, count2):
    return count1 + count2

windowed_stream = win.fold_window("session_window", keyed_stream, clock, windower, builder, folder, merger)

def format_output(item):
    user_id, (window_metadata, total_pages) = item
    return (user_id, json.dumps({
        "user_id": user_id,
        "total_pages": total_pages
    }))

output_stream = op.map("format_output", windowed_stream.down, format_output)

op.output("output", output_stream, FileSink("output.jsonl"))
