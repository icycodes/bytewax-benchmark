from bytewax import testing
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.testing import run_main

flow = Dataflow("test")

def input_gen():
    yield "AAPL", {"timestamp": "2023-01-01T00:00:10Z", "price": 150.0, "volume": 10}

stream = op.input("inp", flow, testing.TestingSource(input_gen()))

clock = wop.EventClock(
    ts_getter=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00")),
    wait_for_system_duration=timedelta(seconds=0)
)
windower = wop.TumblingWindower(length=timedelta(minutes=1), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc))

def builder():
    return {}

def folder(state, item):
    print("FOLDER ITEM:", item)
    return state

def merger(s1, s2):
    return s1

window_out = wop.fold_window("window", stream, clock, windower, builder, folder, merger)
op.inspect("insp", window_out.down)
run_main(flow)
