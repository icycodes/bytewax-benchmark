from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import SlidingWindower, EventClock, fold_window
from bytewax.testing import TestingSource, run_main

flow = Dataflow("test")
data = [
    ("S1", {"time": datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc), "temp": 20.0}),
]
up = op.input("inp", flow, TestingSource(data))

clock = EventClock(
    ts_getter=lambda x: x["time"],
    wait_for_system_duration=timedelta(seconds=0),
)
windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
)

out = fold_window(
    "win", up, clock, windower,
    builder=lambda: [],
    folder=lambda acc, x: acc + [x["temp"]],
    merger=lambda a, b: a + b
)

def rekey(key_item):
    key, (win_id, val) = key_item
    return f"{key}_{win_id}", val

down_rekeyed = op.map("rekey_down", out.down, rekey)
meta_rekeyed = op.map("rekey_meta", out.meta, rekey)

joined = op.join("join", down_rekeyed, meta_rekeyed)

op.inspect("joined", joined)

run_main(flow)
