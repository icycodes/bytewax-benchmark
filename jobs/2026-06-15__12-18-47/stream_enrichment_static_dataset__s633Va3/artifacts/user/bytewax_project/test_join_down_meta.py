import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
from bytewax.testing import TestingSource
from datetime import datetime, timezone, timedelta
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("test")
inp = [
    ("key1", 1, datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)),
    ("key1", 2, datetime(2026, 1, 1, 12, 0, 20, tzinfo=timezone.utc)),
]
stream = op.input("in", flow, TestingSource(inp))
keyed = op.key_on("key", stream, lambda x: x[0])

clock = win.EventClock(lambda x: x[2], wait_for_system_duration=timedelta(seconds=0))
window = win.TumblingWindower(length=timedelta(minutes=1), align_to=datetime(2026, 1, 1, tzinfo=timezone.utc))

out = win.fold_window("fold", keyed, clock, window, lambda: 0, lambda acc, x: acc + x[1], lambda a, b: a + b)

def rekey(item):
    k, (w_id, v) = item
    return (f"{k}_{w_id}", v)

down_rekey = op.map("rekey_down", out.down, rekey)
meta_rekey = op.map("rekey_meta", out.meta, rekey)

joined = op.join("join", down_rekey, meta_rekey)
op.output("out", joined, StdOutSink())

run_main(flow)
