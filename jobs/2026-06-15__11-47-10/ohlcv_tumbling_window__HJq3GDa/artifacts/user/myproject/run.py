import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import TumblingWindower, EventClock, fold_window
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main

# 1. Parsing ISO 8601 timestamps
def parse_timestamp(ts_str):
    ts_str = ts_str.strip()
    if ts_str.endswith('Z'):
        ts_str = ts_str[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        if ' ' in ts_str:
            ts_str = ts_str.replace(' ', 'T')
        dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# 2. Accumulator for OHLCV
@dataclass
class OHLCVAccumulator:
    open_price: float = None
    high_price: float = None
    low_price: float = None
    close_price: float = None
    volume: float = 0.0
    first_timestamp: datetime = None
    last_timestamp: datetime = None

def builder():
    return OHLCVAccumulator()

def folder(acc: OHLCVAccumulator, trade) -> OHLCVAccumulator:
    ts = trade['timestamp']
    price = trade['price']
    vol = trade['volume']
    
    if acc.open_price is None:
        acc.open_price = price
        acc.high_price = price
        acc.low_price = price
        acc.close_price = price
        acc.first_timestamp = ts
        acc.last_timestamp = ts
    else:
        if ts < acc.first_timestamp:
            acc.open_price = price
            acc.first_timestamp = ts
        if ts > acc.last_timestamp:
            acc.close_price = price
            acc.last_timestamp = ts
        if price > acc.high_price:
            acc.high_price = price
        if price < acc.low_price:
            acc.low_price = price
            
    acc.volume += vol
    return acc

def merger(a: OHLCVAccumulator, b: OHLCVAccumulator) -> OHLCVAccumulator:
    if a.open_price is None:
        return b
    if b.open_price is None:
        return a
        
    vol = a.volume + b.volume
    high = max(a.high_price, b.high_price)
    low = min(a.low_price, b.low_price)
    
    if a.first_timestamp <= b.first_timestamp:
        open_p = a.open_price
        first_ts = a.first_timestamp
    else:
        open_p = b.open_price
        first_ts = b.first_timestamp
        
    if b.last_timestamp >= a.last_timestamp:
        close_p = b.close_price
        last_ts = b.last_timestamp
    else:
        close_p = a.close_price
        last_ts = a.last_timestamp
        
    return OHLCVAccumulator(
        open_price=open_p,
        high_price=high,
        low_price=low,
        close_price=close_p,
        volume=vol,
        first_timestamp=first_ts,
        last_timestamp=last_ts
    )

# 3. CSV Source
class CSVSource(DynamicSource):
    def __init__(self, filepath):
        self.filepath = filepath

    def build(self, step_id, worker_index, worker_count):
        return CSVPartition(self.filepath, worker_index, worker_count)

class CSVPartition(StatelessSourcePartition):
    def __init__(self, filepath, worker_index, worker_count):
        self.filepath = filepath
        self.worker_index = worker_index
        self.worker_count = worker_count
        self.file_obj = None
        self.reader = None

    def _open_file(self):
        self.file_obj = open(self.filepath, mode="r", encoding="utf-8")
        self.reader = csv.DictReader(self.file_obj)

    def next_batch(self):
        if self.file_obj is None:
            self._open_file()
            
        batch = []
        for i, row in enumerate(self.reader):
            if i % self.worker_count == self.worker_index:
                if not row.get('timestamp') or not row.get('symbol') or not row.get('price') or not row.get('volume'):
                    continue
                try:
                    ts = parse_timestamp(row['timestamp'])
                    symbol = row['symbol'].strip()
                    price = float(row['price'])
                    volume = float(row['volume'])
                    batch.append({
                        'timestamp': ts,
                        'symbol': symbol,
                        'price': price,
                        'volume': volume
                    })
                except Exception:
                    continue
                
                if len(batch) >= 100:
                    return batch
        if len(batch) > 0:
            return batch
        raise StopIteration()

    def close(self):
        if self.file_obj is not None:
            self.file_obj.close()

# 4. JSONL Sink
class JSONLSink(DynamicSink):
    def __init__(self, filepath):
        self.filepath = filepath

    def build(self, step_id, worker_index, worker_count):
        return JSONLPartition(self.filepath)

class JSONLPartition(StatelessSinkPartition):
    def __init__(self, filepath):
        self.filepath = filepath
        self.file_obj = None

    def write_batch(self, items):
        if self.file_obj is None:
            self.file_obj = open(self.filepath, mode="a", encoding="utf-8")
        for item in items:
            self.file_obj.write(json.dumps(item) + "\n")
            self.file_obj.flush()

    def close(self):
        if self.file_obj is not None:
            self.file_obj.close()

def main():
    parser = argparse.ArgumentParser(description="Tumbling Window OHLCV Aggregation")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output JSONL file path")
    args = parser.parse_args()

    # Pre-truncate the output file to ensure clean runs
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("")

    flow = Dataflow("ohlcv_aggregation")
    
    # Input stream
    inp = op.input("input", flow, CSVSource(args.input))
    
    # Key stream by symbol
    keyed = op.key_on("key_on", inp, lambda trade: trade['symbol'])
    
    # Define clock and windower
    clock = EventClock(
        ts_getter=lambda trade: trade['timestamp'],
        wait_for_system_duration=timedelta(seconds=0),
    )
    
    windower = TumblingWindower(
        length=timedelta(minutes=1),
        align_to=datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    )
    
    # Fold window to compute OHLCV
    windowed = fold_window(
        "fold_window",
        keyed,
        clock,
        windower,
        builder=builder,
        folder=folder,
        merger=merger
    )
    
    # Key both down and meta streams to join by f"{symbol}:{window_id}"
    down_keyed = op.map("down_keyed", windowed.down, lambda x: (f"{x[0]}:{x[1][0]}", (x[0], x[1][1])))
    meta_keyed = op.map("meta_keyed", windowed.meta, lambda x: (f"{x[0]}:{x[1][0]}", x[1][1]))
    
    # Join the streams
    joined = op.join("join_streams", down_keyed, meta_keyed)
    
    # Format to output JSON format
    def format_output(item):
        key, ((symbol, acc), metadata) = item
        return {
            "window_start": metadata.open_time.isoformat(),
            "symbol": symbol,
            "open": acc.open_price,
            "high": acc.high_price,
            "low": acc.low_price,
            "close": acc.close_price,
            "volume": acc.volume
        }
        
    formatted = op.map("format_output", joined, format_output)
    
    # Output to JSONL
    op.output("output", formatted, JSONLSink(args.output))
    
    # Run the dataflow
    run_main(flow)

if __name__ == "__main__":
    main()
