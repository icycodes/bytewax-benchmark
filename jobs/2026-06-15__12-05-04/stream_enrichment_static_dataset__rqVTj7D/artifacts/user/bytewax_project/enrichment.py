"""
Stream enrichment and windowed aggregation pipeline using Bytewax 0.21.

Pipeline:
  1. Read transaction events line-by-line from `transactions.jsonl`.
  2. Parse each JSON line into a dict.
  3. Enrich each transaction with `category` and `price` from the static
     `products.json` dataset; drop the transaction if the product is unknown.
  4. Key the enriched stream by `category`.
  5. Apply a 1-minute tumbling EventClock window (aligned to the start of the
     hour) and sum revenue (price * quantity) inside each window.
  6. Join each windowed result with its WindowMetadata so we know the
     window open_time, then format and emit a JSON string to stdout.
"""

import json
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# 1.  Load static product catalogue at module (import) time.
#     This runs once on every worker before the dataflow starts.
# ---------------------------------------------------------------------------
with open("products.json") as _fh:
    PRODUCTS: dict = json.load(_fh)


# ---------------------------------------------------------------------------
# 2.  Helper functions
# ---------------------------------------------------------------------------

def parse_and_enrich(line: str):
    """Parse a JSONL line and enrich it with product metadata.

    Returns a tuple ``(category, revenue)`` ready for windowing, or
    ``None`` if the product_id is not found in the catalogue (the item
    will then be dropped by ``filter_map``).
    """
    txn = json.loads(line)
    product = PRODUCTS.get(txn["product_id"])
    if product is None:
        return None  # drop unknown products

    category: str = product["category"]
    price: float = product["price"]
    quantity: int = txn["quantity"]
    ts: datetime = datetime.fromisoformat(txn["timestamp"].replace("Z", "+00:00"))

    # The windowing operators need ``(key, value)`` pairs where the value
    # carries enough information for both the clock (timestamp) and the
    # accumulator (revenue).  We embed the timestamp inside the value so
    # that ``EventClock.ts_getter`` can extract it.
    return (category, (ts, price * quantity))


def get_timestamp(item) -> datetime:
    """Extract the event timestamp from a ``(ts, revenue)`` value tuple."""
    ts, _revenue = item
    return ts


def revenue_folder(acc: float, item) -> float:
    """Add the revenue of a single transaction to the running total."""
    _ts, revenue = item
    return acc + revenue


def revenue_merger(acc1: float, acc2: float) -> float:
    """Merge two partial revenue accumulators (used when workers merge state)."""
    return acc1 + acc2


def format_result(key_window_revenue) -> str:
    """Format a ``(category, (window_id, revenue))`` tuple as a JSON string.

    The windowing operator emits items on the ``down`` stream with the
    shape ``(key, (window_id, aggregated_value))``.  We need the
    WindowMetadata (``open_time``) from the ``meta`` stream, but
    ``fold_window`` does not attach it to ``down``.  Instead we use
    ``join_window`` to combine both streams, or—simpler—we compute the
    window_start from the window_id directly.

    Actually the simplest approach: we join the ``down`` stream with
    the ``meta`` stream using ``op.join`` on ``(category, window_id)``
    as the key, which is exactly what ``WindowOut.meta`` provides.
    However, for a bounded / batch-style file source the cleanest
    solution is to use ``collect_window`` on the meta stream, or just
    derive the window start from the ``open_time`` field on the
    ``WindowMetadata`` objects available in the meta output.

    Here we handle this by zipping ``down`` and ``meta`` through a
    stateful join step.  See the main dataflow construction below.
    """
    # This function is called after the join; see dataflow construction.
    category, ((_win_id_r, revenue), (_win_id_m, meta)) = key_window_revenue
    window_start: str = meta.open_time.isoformat()
    return json.dumps(
        {"category": category, "window_start": window_start, "revenue": revenue}
    )


# ---------------------------------------------------------------------------
# 3.  Dataflow definition
# ---------------------------------------------------------------------------

#: Align all tumbling windows to the top of any hour.  The exact date does
#: not matter as long as the datetime is on an exact hour boundary in UTC;
#: the windower will repeat the pattern every ``length``.
ALIGN_TO = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

flow = Dataflow("enrichment")

# -- Input: read transactions.jsonl line-by-line ----------------------------
raw = op.input("input", flow, FileSource("transactions.jsonl"))

# -- Parse + enrich (drop unknown products) ---------------------------------
enriched = op.filter_map("enrich", raw, parse_and_enrich)

# -- Define the clock and windower ------------------------------------------
clock = win.EventClock(
    ts_getter=get_timestamp,
    # For a bounded / batch file source we do not need to wait for late
    # events; set the waiting duration to zero so that windows close as
    # soon as the watermark advances past them.
    wait_for_system_duration=timedelta(seconds=0),
)

windower = win.TumblingWindower(
    length=timedelta(minutes=1),
    align_to=ALIGN_TO,
)

# -- Windowed revenue aggregation -------------------------------------------
# ``fold_window`` returns a ``WindowOut`` with two relevant streams:
#   * ``down``  – ``(category, (window_id, total_revenue))``
#   * ``meta``  – ``(category, (window_id, WindowMetadata))``
windowed = win.fold_window(
    "revenue_window",
    enriched,
    clock,
    windower,
    builder=lambda: 0.0,
    folder=revenue_folder,
    merger=revenue_merger,
)

# -- Attach window metadata so we can emit the window_start -----------------
# Both ``windowed.down`` and ``windowed.meta`` carry items of the shape
# ``(category, (window_id, payload))``.  We re-key both streams on the
# composite ``"category:window_id"`` string so that a simple stateful join
# can pair them up, then format the combined record.

def rekey_with_window(item):
    """Re-key ``(cat, (wid, payload))`` → ``("cat:wid", (wid, payload))``."""
    category, (window_id, payload) = item
    composite_key = f"{category}:{window_id}"
    return (composite_key, (window_id, payload))


rekeyed_revenue = op.map("rekey_revenue", windowed.down, rekey_with_window)
rekeyed_meta = op.map("rekey_meta", windowed.meta, rekey_with_window)

# ``op.join`` emits ``(key, (val_left, val_right))`` once both sides arrive.
joined = op.join("join_meta", rekeyed_revenue, rekeyed_meta)

# -- Format output ----------------------------------------------------------
def format_joined(item) -> str:
    """Turn ``("cat:wid", ((wid, revenue), (wid, meta)))`` into a JSON string."""
    composite_key, ((window_id, revenue), (_wid2, meta)) = item
    category = composite_key.rsplit(":", 1)[0]
    window_start: str = meta.open_time.isoformat()
    return json.dumps(
        {"category": category, "window_start": window_start, "revenue": revenue}
    )


formatted = op.map("format", joined, format_joined)

# -- Output to stdout -------------------------------------------------------
op.output("output", formatted, StdOutSink())
