import argparse
import csv
import json
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main

class CSVPartition(StatelessSourcePartition):
    def __init__(self, path):
        self.f = open(path, "r", newline="")
        self.reader = csv.DictReader(self.f)

    def next_batch(self):
        try:
            return [next(self.reader)]
        except StopIteration:
            raise StopIteration

    def close(self):
        self.f.close()

class CSVSource(DynamicSource):
    def __init__(self, path):
        self.path = path

    def build(self, step_id, worker_index, worker_count):
        return CSVPartition(self.path)

class JSONLPartition(StatelessSinkPartition):
    def __init__(self, path):
        self.f = open(path, "w", newline="")

    def write_batch(self, items):
        for item in items:
            self.f.write(json.dumps(item) + "\n")

    def close(self):
        self.f.close()

class JSONLSink(DynamicSink):
    def __init__(self, path):
        self.path = path

    def build(self, step_id, worker_index, worker_count):
        return JSONLPartition(self.path)

def extract_ts(item):
    ts_str = item["timestamp"]
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)

def builder():
    return {
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": 0.0,
        "min_ts": None,
        "max_ts": None,
        "window_start": None,
    }

def folder(state, item):
    price = float(item["price"])
    vol = float(item["volume"])
    ts = extract_ts(item)
    
    if state["window_start"] is None:
        window_start = ts.replace(second=0, microsecond=0)
        ws_str = window_start.isoformat()
        if ws_str.endswith("+00:00"):
            ws_str = ws_str.replace("+00:00", "Z")
        state["window_start"] = ws_str

    if state["high"] is None or price > state["high"]:
        state["high"] = price
    if state["low"] is None or price < state["low"]:
        state["low"] = price
        
    state["volume"] += vol
    
    if state["min_ts"] is None or ts < state["min_ts"]:
        state["min_ts"] = ts
        state["open"] = price
        
    if state["max_ts"] is None or ts >= state["max_ts"]:
        state["max_ts"] = ts
        state["close"] = price
        
    return state

def merger(s1, s2):
    if s1["min_ts"] is None:
        return s2
    if s2["min_ts"] is None:
        return s1
        
    return {
        "window_start": s1["window_start"] or s2["window_start"],
        "open": s1["open"] if s1["min_ts"] < s2["min_ts"] else s2["open"],
        "close": s1["close"] if s1["max_ts"] >= s2["max_ts"] else s2["close"],
        "high": max(s1["high"], s2["high"]),
        "low": min(s1["low"], s2["low"]),
        "volume": s1["volume"] + s2["volume"],
        "min_ts": min(s1["min_ts"], s2["min_ts"]),
        "max_ts": max(s1["max_ts"], s2["max_ts"]),
    }

def format_output(key__win_state):
    symbol, (win_id, state) = key__win_state
    return {
        "window_start": state["window_start"],
        "symbol": symbol,
        "open": state["open"],
        "high": state["high"],
        "low": state["low"],
        "close": state["close"],
        "volume": state["volume"]
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    flow = Dataflow("ohlcv")
    
    stream = op.input("in", flow, CSVSource(args.input))
    
    keyed_stream = op.key_on("key_by_symbol", stream, lambda x: x["symbol"])
    
    clock = wop.EventClock(
        ts_getter=extract_ts,
        wait_for_system_duration=timedelta(seconds=0)
    )
    
    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
    windower = wop.TumblingWindower(
        length=timedelta(minutes=1),
        align_to=align_to
    )
    
    window_out = wop.fold_window(
        "window",
        keyed_stream,
        clock,
        windower,
        builder,
        folder,
        merger
    )
    
    out_stream = op.map("format_output", window_out.down, format_output)
    
    op.output("out", out_stream, JSONLSink(args.output))
    
    run_main(flow)

if __name__ == "__main__":
    main()
