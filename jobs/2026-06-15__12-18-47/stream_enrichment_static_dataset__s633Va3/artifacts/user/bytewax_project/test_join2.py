import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import run_main
from bytewax.testing import TestingSource
from datetime import datetime, timezone, timedelta

def get_event_time(tx):
    return tx["timestamp"]

flow = Dataflow("test")
inp = [
    {"category": "A", "timestamp": datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc), "val": 10},
    {"category": "A", "timestamp": datetime(2026, 1, 1, 12, 0, 20, tzinfo=timezone.utc), "val": 20},
]
stream = op.input("in", flow, TestingSource(inp))
keyed = op.map("key", stream, lambda x: (x["category"], x["val"]))

clock = win.EventClock(get_event_time, wait_for_system_duration=timedelta(seconds=0))
window = win.TumblingWindower(length=timedelta(minutes=1), align_to=datetime(2026, 1, 1, tzinfo=timezone.utc))

out = win.reduce_window("reduce", keyed, clock, window, lambda a, b: a + b)

def rekey(item):
    k, (w_id, v) = item
    return (f"{k}_{w_id}", v)

down_rekey = op.map("rekey_down", out.down, rekey)
meta_rekey = op.map("rekey_meta", out.meta, rekey)

joined = op.join("join", down_rekey, meta_rekey)

def format_out(item):
    k, (v, meta) = item
    return (k, meta.open_time, v)

final = op.map("format", joined, format_out)
op.output("out", final, StdOutSink())

run_main(flow)
