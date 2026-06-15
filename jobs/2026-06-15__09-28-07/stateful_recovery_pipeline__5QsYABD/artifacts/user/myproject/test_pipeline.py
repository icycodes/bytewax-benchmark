import json
from datetime import datetime, timezone, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource
from bytewax.operators.windowing import EventClock, SlidingWindower

def parse_input(line: str):
    data = json.loads(line)
    return (data["sensor_id"], data)

def extract_time(item):
    return datetime.fromisoformat(item["time"].replace("Z", "+00:00"))

def builder():
    return []

def folder(acc, item):
    acc.append(item["temp"])
    return acc

def merger(acc1, acc2):
    acc1.extend(acc2)
    return acc1

flow = Dataflow("pipeline")
inp = op.input("in", flow, FileSource("input.jsonl"))
parsed = op.map("parse", inp, parse_input)
clock = EventClock(extract_time, wait_for_system_duration=timedelta(seconds=0))
align_to = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=align_to
)
windowed = win.fold_window("window", parsed, clock, windower, builder, folder, merger)
op.inspect("inspect_meta", windowed.meta)
