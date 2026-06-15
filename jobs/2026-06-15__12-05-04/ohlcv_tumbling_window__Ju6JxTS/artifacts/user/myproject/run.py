"""
Tumbling-window OHLCV aggregation with Bytewax 0.21.

Usage:
    python run.py --input input.csv --output output.jsonl
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import CSVSource, FileSink
from bytewax.dataflow import Dataflow


# ---------------------------------------------------------------------------
# OHLCV accumulator
# ---------------------------------------------------------------------------

@dataclass
class OHLCV:
    """Mutable accumulator for one OHLCV bar."""

    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0
    # Track trade count so we know whether the bar is still empty.
    _count: int = field(default=0, repr=False)

    # --- fold helpers -------------------------------------------------------

    @staticmethod
    def builder() -> "OHLCV":
        return OHLCV()

    @staticmethod
    def folder(acc: "OHLCV", trade: dict) -> "OHLCV":
        price = float(trade["price"])
        volume = float(trade["volume"])

        if acc._count == 0:
            acc.open = price
        acc.high = max(acc.high, price)
        acc.low = min(acc.low, price)
        acc.close = price          # last price seen (stream is ordered)
        acc.volume += volume
        acc._count += 1
        return acc

    @staticmethod
    def merger(a: "OHLCV", b: "OHLCV") -> "OHLCV":
        """Merge two partial OHLCV accumulators (used during redistribution)."""
        if a._count == 0:
            return b
        if b._count == 0:
            return a
        merged = OHLCV()
        merged.open = a.open          # a arrived first (ordered fold)
        merged.high = max(a.high, b.high)
        merged.low = min(a.low, b.low)
        merged.close = b.close        # b arrived last
        merged.volume = a.volume + b.volume
        merged._count = a._count + b._count
        return merged


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_ts(row: dict) -> datetime:
    """Parse ISO 8601 timestamp from a CSV row into an aware UTC datetime."""
    ts_str = row["timestamp"]
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def row_to_keyed(row: dict):
    """Return (symbol, row) so the stream is keyed by ticker."""
    return row["symbol"]


# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------

def build_flow(input_path: Path, output_path: Path) -> Dataflow:
    # Align all 1-minute windows to the Unix epoch (UTC midnight 1970-01-01).
    ALIGN_TO = datetime(1970, 1, 1, tzinfo=timezone.utc)
    WINDOW_LEN = timedelta(minutes=1)
    # Allow up to 10 s of system-clock lag before closing a window.
    WAIT = timedelta(seconds=10)

    flow = Dataflow("ohlcv")

    # 1. Read CSV ─────────────────────────────────────────────────────────────
    rows: op.Stream = op.input("csv_in", flow, CSVSource(input_path))
    # CSVSource yields dicts with string values for each column.

    # 2. Key by symbol ────────────────────────────────────────────────────────
    keyed = op.key_on("key_by_symbol", rows, row_to_keyed)

    # 3. Define event clock & tumbling windower ────────────────────────────────
    clock = win.EventClock(
        ts_getter=lambda row: parse_ts(row),
        wait_for_system_duration=WAIT,
    )
    windower = win.TumblingWindower(length=WINDOW_LEN, align_to=ALIGN_TO)

    # 4. Fold trades into OHLCV bars ──────────────────────────────────────────
    window_out: win.WindowOut = win.fold_window(
        "ohlcv_window",
        keyed,
        clock,
        windower,
        OHLCV.builder,
        OHLCV.folder,
        OHLCV.merger,
    )

    # window_out.down  → Stream[Tuple[str, Tuple[window_id, OHLCV]]]
    # window_out.meta  → Stream[Tuple[str, Tuple[window_id, WindowMetadata]]]

    # 5. Zip OHLCV values with their window metadata ──────────────────────────
    # We tag each stream with the window_id so we can join them.
    def tag_value(item):
        symbol, (wid, ohlcv) = item
        return (f"{symbol}:{wid}", ohlcv)

    def tag_meta(item):
        symbol, (wid, meta) = item
        return (f"{symbol}:{wid}", (symbol, meta))

    tagged_values = op.map("tag_values", window_out.down, tag_value)
    tagged_metas = op.map("tag_metas", window_out.meta, tag_meta)

    # Strategy: flatten both streams into a single keyed stream with a type tag,
    # then use stateful_flat_map to buffer until both pieces arrive.

    def label_value(item):
        key, ohlcv = item
        return (key, ("V", ohlcv))

    def label_meta(item):
        key, sym_meta = item
        return (key, ("M", sym_meta))

    lv = op.map("label_v", tagged_values, label_value)
    lm = op.map("label_m", tagged_metas, label_meta)

    merged = op.merge("merge_vm", lv, lm)

    # stateful_flat_map: accumulate {V: ohlcv, M: (symbol, meta)} per key,
    # emit one record when both are present.
    def joiner(
        state: Optional[dict], tagged: tuple
    ) -> tuple[Optional[dict], list[str]]:
        tag, payload = tagged
        if state is None:
            state = {}
        state[tag] = payload

        if "V" in state and "M" in state:
            ohlcv: OHLCV = state["V"]
            symbol, meta = state["M"]
            record = {
                "window_start": meta.open_time.isoformat(),
                "symbol": symbol,
                "open": ohlcv.open,
                "high": ohlcv.high,
                "low": ohlcv.low,
                "close": ohlcv.close,
                "volume": ohlcv.volume,
            }
            return None, [json.dumps(record)]   # reset state, emit one line

        return state, []

    joined = op.stateful_flat_map("join_vm", merged, joiner)

    # 6. Write JSONL ──────────────────────────────────────────────────────────
    # FileSink accepts Stream[Tuple[key, value]] and writes only the value.
    op.output("jsonl_out", joined, FileSink(output_path))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate trade CSV into 1-minute OHLCV bars (JSONL output)."
    )
    parser.add_argument("--input", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output", required=True, help="Path to the output JSONL file.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    flow = build_flow(input_path, output_path)

    # Run single-process (no cluster needed for file I/O).
    from bytewax.testing import run_main
    run_main(flow)


if __name__ == "__main__":
    main()
