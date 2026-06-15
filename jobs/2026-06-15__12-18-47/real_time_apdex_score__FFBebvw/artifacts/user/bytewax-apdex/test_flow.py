from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import TumblingWindower, EventClock, fold_window
from bytewax.testing import TestingSource, run_main

flow = Dataflow("test")
data = [
    ("auth", 100, datetime(2023, 1, 1, 12, 0, 5, tzinfo=timezone.utc)),
    ("auth", 600, datetime(2023, 1, 1, 12, 0, 15, tzinfo=timezone.utc)),
]

up = op.input("input", flow, TestingSource(data))
keyed = op.key_on("key_on", up, lambda x: x[0])

clock = EventClock(
    ts_getter=lambda x: x[2],
    wait_for_system_duration=timedelta(seconds=0)
)
windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
)

windowed = fold_window(
    "fold", keyed, clock, windower,
    builder=lambda: [],
    folder=lambda acc, x: acc + [x[1]],
    merger=lambda a, b: a + b
)

def rekey(x):
    key, (win_id, val) = x
    return (f"{key}_{win_id}", val)

down_rekeyed = op.map("rekey_down", windowed.down, rekey)
meta_rekeyed = op.map("rekey_meta", windowed.meta, rekey)

joined = op.join("join", down_rekeyed, meta_rekeyed)
op.inspect("out", joined)

run_main(flow)
