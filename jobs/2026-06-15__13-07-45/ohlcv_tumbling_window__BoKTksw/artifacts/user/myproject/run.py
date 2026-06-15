"""
Bytewax dataflow for 1-minute tumbling window OHLCV aggregation.

Reads a CSV file of trades, groups by symbol, and calculates
Open, High, Low, Close, Volume over 1-minute tumbling windows
aligned to the minute boundary.
"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    WindowOut,
    fold_window,
)
from bytewax.outputs import DynamicSink, StatelessSinkPartition


@dataclass
class Trade:
    """Represents a single trade."""
    timestamp: datetime
    symbol: str
    price: float
    volume: float


@dataclass
class OHLCVState:
    """Mutable accumulator for OHLCV window calculations."""
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_timestamp: datetime
    last_timestamp: datetime

    @classmethod
    def from_trade(cls, trade: Trade) -> "OHLCVState":
        """Initialize state from the first trade in a window."""
        return cls(
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.volume,
            first_timestamp=trade.timestamp,
            last_timestamp=trade.timestamp,
        )

    def update(self, trade: Trade) -> None:
        """Incorporate a subsequent trade into the accumulator."""
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.volume
        self.last_timestamp = trade.timestamp


# Parse the header once to know the field names
CSV_HEADER = ["timestamp", "symbol", "price", "volume"]


def parse_csv_line(line: str) -> Trade:
    """Parse a single CSV line into a Trade object."""
    parts = line.strip().split(",")
    if len(parts) != 4:
        raise ValueError(f"Expected 4 fields, got {len(parts)}: {line}")
    ts_str, symbol, price_str, volume_str = parts
    # Parse ISO 8601 timestamp with optional 'Z' suffix
    ts_str = ts_str.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(ts_str)
    return Trade(
        timestamp=timestamp,
        symbol=symbol,
        price=float(price_str),
        volume=float(volume_str),
    )


def extract_timestamp(trade: Trade) -> datetime:
    """Extract event timestamp from a trade for the EventClock."""
    return trade.timestamp


def builder() -> Optional[OHLCVState]:
    """Create an empty accumulator. Returns None for windows with no trades."""
    return None


def folder(acc: Optional[OHLCVState], trade: Trade) -> OHLCVState:
    """Fold a trade into the OHLCV accumulator."""
    if acc is None:
        return OHLCVState.from_trade(trade)
    acc.update(trade)
    return acc


def merger(a: Optional[OHLCVState], b: Optional[OHLCVState]) -> Optional[OHLCVState]:
    """Merge two OHLCV accumulators (for session windows, not used with tumbling)."""
    if a is None:
        return b
    if b is None:
        return a
    # Determine which trade came first
    if a.first_timestamp <= b.first_timestamp:
        first_state, second_state = a, b
    else:
        first_state, second_state = b, a

    merged = OHLCVState(
        open=first_state.open,
        high=max(a.high, b.high),
        low=min(a.low, b.low),
        close=second_state.close if second_state.last_timestamp >= first_state.last_timestamp else first_state.close,
        volume=a.volume + b.volume,
        first_timestamp=min(a.first_timestamp, b.first_timestamp),
        last_timestamp=max(a.last_timestamp, b.last_timestamp),
    )
    return merged


class JSONLSinkPartition(StatelessSinkPartition[dict]):
    """Writes JSON objects as individual JSON lines to a file."""

    def __init__(self, path: Path):
        self._file = path.open("a")

    def write_batch(self, items: List[dict]) -> None:
        for item in items:
            self._file.write(json.dumps(item) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


class JSONLSink(DynamicSink[dict]):
    """Dynamic sink that creates JSONL file partitions."""

    def __init__(self, path: Path):
        self._path = path

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> JSONLSinkPartition:
        return JSONLSinkPartition(self._path)


def build_dataflow(input_path: Path, output_path: Path) -> Dataflow:
    """Construct the Bytewax dataflow for OHLCV aggregation."""
    flow = Dataflow("ohlcv_aggregation")

    # 1. Read input CSV lines
    lines = op.input("read_csv", flow, FileSource(input_path))

    # Skip the header line
    def is_data_line(line: str) -> bool:
        return not line.startswith("timestamp,")

    data_lines = op.filter("skip_header", lines, is_data_line)

    # 2. Parse CSV lines into Trade objects
    trades = op.map("parse_trade", data_lines, parse_csv_line)

    # 3. Key the stream by symbol
    keyed = op.key_on("key_by_symbol", trades, lambda t: t.symbol)

    # 4. Define the clock and windower
    # Use a minimal wait_for_system_duration so we don't wait for late data
    clock = EventClock(
        ts_getter=extract_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # Align to the epoch start of the minute boundary
    align_to = datetime(1970, 1, 1, tzinfo=timezone.utc)
    windower = TumblingWindower(
        length=timedelta(minutes=1),
        align_to=align_to,
    )

    # 5. Apply fold_window to accumulate OHLCV state per window
    window_out: WindowOut = fold_window(
        "window_aggregation",
        keyed,
        clock,
        windower,
        builder=builder,
        folder=folder,
        merger=merger,
    )

    # 6. Map the window output to final OHLCV dicts
    # window_out.down emits: (symbol, (window_id, OHLCVState))
    # We need to derive window_start from the state's first_timestamp
    # by truncating to the minute boundary.
    def to_ohlcv_dict(item: tuple) -> dict:
        symbol, (window_id, state) = item
        if state is None:
            return None
        # Truncate to minute boundary for the window start
        window_start = state.first_timestamp.replace(second=0, microsecond=0)
        return {
            "window_start": window_start.isoformat(),
            "symbol": symbol,
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "volume": state.volume,
        }

    ohlcv_dicts = op.filter_map("to_dict", window_out.down, to_ohlcv_dict)

    # 7. Write to JSONL output
    op.output("write_jsonl", ohlcv_dicts, JSONLSink(output_path))

    return flow


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate trade data into 1-minute OHLCV bars."
    )
    parser.add_argument(
        "--input", required=True, type=Path, help="Path to input CSV file."
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Path to output JSONL file."
    )
    args = parser.parse_args()

    # Clear the output file if it exists
    args.output.write_text("")

    flow = build_dataflow(args.input, args.output)

    # Use run_main for single-process execution
    from bytewax.testing import run_main

    run_main(flow)


if __name__ == "__main__":
    main()
