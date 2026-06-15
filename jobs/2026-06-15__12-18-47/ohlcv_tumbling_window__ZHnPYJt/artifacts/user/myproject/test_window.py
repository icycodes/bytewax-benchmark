from bytewax import testing
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.testing import run_main

flow = Dataflow("test")

def input_gen():
    yield "AAPL", {"timestamp": "2023-01-01T00:00:10Z", "price": 150.0, "volume": 10}
    yield "AAPL", {"timestamp": "2023-01-01T00:00:50Z", "price": 155.0, "volume": 5}
    yield "AAPL", {"timestamp": "2023-01-01T00:01:10Z", "price": 160.0, "volume": 20}

stream = op.input("inp", flow, testing.TestingSource(input_gen()))

def extract_ts(item):
    return datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))

clock = wop.EventClock(
    ts_getter=extract_ts,
    wait_for_system_duration=timedelta(seconds=0)
)

align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
windower = wop.TumblingWindower(length=timedelta(minutes=1), align_to=align_to)

def builder():
    return {"count": 0}

def folder(state, item):
    state["count"] += 1
    return state

def merger(s1, s2):
    return {"count": s1["count"] + s2["count"]}

window_out = wop.fold_window(
    "window",
    stream,
    clock,
    windower,
    builder,
    folder,
    merger
)

op.inspect("down", window_out.down)
op.inspect("meta", window_out.meta)

run_main(flow)
