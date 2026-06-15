"""
Bytewax dataflow for enriching transactions with product metadata
and computing windowed revenue per category.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, TumblingWindower, fold_window
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink


# Load static product dataset at module initialization time
def _load_products() -> dict:
    with open("products.json", "r") as f:
        return json.load(f)


PRODUCTS: dict = _load_products()


def _deserialize_transaction(line: str) -> dict:
    """Parse a JSON line into a transaction dict."""
    return json.loads(line)


def _enrich_transaction(txn: dict) -> dict | None:
    """Enrich transaction with product metadata. Returns None if product_id not found."""
    product = PRODUCTS.get(txn["product_id"])
    if product is None:
        return None
    txn["category"] = product["category"]
    txn["price"] = product["price"]
    return txn


def _key_on_category(txn: dict) -> str:
    """Extract the category as the key for windowing."""
    return txn["category"]


def _parse_timestamp(txn: dict) -> datetime:
    """Parse the transaction timestamp into a timezone-aware datetime."""
    ts = txn["timestamp"]
    # Handle 'Z' suffix (UTC indicator)
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    # Ensure timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _compute_revenue(txn: dict) -> float:
    """Compute the revenue contribution of a transaction."""
    return txn["price"] * txn["quantity"]


def _format_output(key__window_id__revenue: tuple) -> str:
    """Format the windowed aggregation result as a JSON string."""
    category, (window_id, metadata_and_revenue) = key__window_id__revenue
    # The value is a tuple of (window_metadata, revenue) after join
    # But we're using fold_window directly, so the value is (window_id, revenue)
    # Actually the down stream emits (original_key, (window_id, accumulator))
    pass


def flow() -> Dataflow:
    flow = Dataflow("enrichment")

    # 1. Read transactions from JSONL file
    lines = op.input("transactions_input", flow, FileSource("transactions.jsonl"))

    # 2. Deserialize JSON lines
    transactions = op.map("deserialize", lines, _deserialize_transaction)

    # 3. Enrich with product metadata, dropping unmatched transactions
    enriched = op.filter_map("enrich", transactions, _enrich_transaction)

    # 4. Key by category for windowed aggregation
    keyed = op.key_on("key_on_category", enriched, _key_on_category)

    # 5. Define event-time clock (based on transaction timestamp)
    clock = EventClock(
        ts_getter=_parse_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    # 6. Define 1-minute tumbling window, aligned to the start of the hour
    windower = TumblingWindower(
        length=timedelta(minutes=1),
        align_to=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )

    # 7. Fold: accumulate revenue per category per window
    #    The value flowing through is the revenue contribution of each transaction
    revenue_stream = op.map_value("compute_revenue", keyed, _compute_revenue)

    windowed = fold_window(
        "windowed_revenue",
        revenue_stream,
        clock,
        windower,
        builder=lambda: 0.0,
        folder=lambda acc, revenue: acc + revenue,
        merger=lambda a, b: a + b,
    )

    # 8. Format and output results
    # windowed.down is KeyedStream where key is (category, window_id) and value is accumulator (revenue)
    # We need to combine with windowed.meta to get window open_time
    # Strategy: use map_value on down, but we also need window metadata.
    # Alternative: collect_window to get list of transactions, but we want fold.
    # 
    # The approach: join windowed.down with windowed.meta on window_id
    # Both are keyed by (category, window_id) — but meta has different key structure.
    # 
    # Actually, let's look at the output of fold_window more carefully:
    # down: KeyedStream[Tuple[int, W_co]] — key is (window_id,) and value is accumulator
    # But wait, the docstring example shows: ('key1', (0, 3))
    # This means the output preserves the original key.
    #
    # Let me use a different approach: use map on the down stream and format directly.
    # The down stream's key is the original key (category) and the value is (window_id, revenue).
    
    # Actually I need to reconsider. Let me use collect_window instead which collects all 
    # transactions, then I can compute the revenue and access window metadata via join.
    # 
    # OR: I can use fold_window and then join with meta.
    # The meta stream is keyed by (window_id,) and emits WindowMetadata.
    # The down stream is keyed by (category, window_id) or just (category,)?
    #
    # Let me test this with a simpler approach: use fold_window output directly
    # and access the window metadata through the meta stream join.
    
    # Actually, the simplest approach: use join on the down and meta streams
    # Both need to be keyed by window_id for the join to work.
    
    # Let me re-key the down stream to use window_id as key
    def key_on_window_id(cat__winid_rev):
        category, (window_id, revenue) = cat__winid_rev
        return (window_id, (category, revenue))
    
    # Re-key down stream by window_id
    down_rekeyed = op.map("rekey_down", windowed.down, key_on_window_id)
    down_by_window = op.key_on("key_down_by_window", down_rekeyed, lambda x: x[0])
    
    # meta stream is already keyed by window_id
    # Join them
    def join_down_meta(window_id__down_and_meta):
        window_id, (down_val, meta) = window_id__down_and_meta
        category, revenue = down_val
        return {
            "category": category,
            "window_start": meta.open_time.isoformat(),
            "revenue": revenue,
        }
    
    joined = op.join("join_with_meta", down_by_window, windowed.meta)
    formatted = op.map("format_output", joined, join_down_meta)
    
    # Serialize to JSON string
    output = op.map("to_json", formatted, json.dumps)
    
    op.output("stdout_sink", output, StdOutSink())

    return flow
