"""
Real-time Top-3 product view aggregation pipeline using Bytewax.

Reads product view events from input.json, maintains running counts
per product, and tracks the global Top-3 most-viewed products.

Recovery: initialize SQLite partitions before first run with:
    python -m bytewax.recovery ./recovery_dir 1
Then run with:
    python -m bytewax.run pipeline:flow -r ./recovery_dir -s 10 -b 0
"""

import json
import copy

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

# ---------------------------------------------------------------------------
# Load events from input.json
# ---------------------------------------------------------------------------
with open("input.json") as fh:
    _raw_events = json.load(fh)

# Each event is [product_id, increment]; convert to (str, int) tuples.
_events = [(_item[0], int(_item[1])) for _item in _raw_events]

# ---------------------------------------------------------------------------
# Step 1: per-product running count
# ---------------------------------------------------------------------------

def _count_mapper(state, increment):
    """Accumulate view counts per product.

    State: int  (total views so far; None means first time we see this key)
    Returns: (new_state, (product_id_is_the_key, new_total))
    The output value is the new total count for this product.
    """
    if state is None:
        state = 0
    state = state + increment
    return (state, state)


# ---------------------------------------------------------------------------
# Step 2: global Top-3 maintenance
# ---------------------------------------------------------------------------
# We re-key every update to the constant string "global" so that a single
# stateful operator sees every product count and can maintain the Top-3.

def _reroute_to_global(key_and_count):
    """Re-key the (product_id, total_count) pair to the constant "global"."""
    product_id, total_count = key_and_count
    # value we pass downstream is (product_id, total_count)
    return ("global", (product_id, total_count))


def _topk_mapper(state, update):
    """Maintain the global Top-3 products by total view count.

    State: dict mapping product_id -> total_count
    update: (product_id, latest_total_count) for that product
    Returns: (new_state, top3_list)
        top3_list is a list of [product_id, count] sorted desc by count,
        limited to the top 3 entries.
    """
    if state is None:
        state = {}
    else:
        # Always work on a fresh copy to avoid snapshot corruption.
        state = dict(state)

    product_id, total_count = update
    state[product_id] = total_count

    # Sort all products by count descending, take top 3.
    top3 = sorted(state.items(), key=lambda kv: kv[1], reverse=True)[:3]
    # Convert to list of [product_id, count] for JSON serialisation.
    top3_list = [[pid, cnt] for pid, cnt in top3]

    return (state, top3_list)


# ---------------------------------------------------------------------------
# Output formatter
# ---------------------------------------------------------------------------

def _format_output(key_and_top3):
    """Serialize the Top-3 result to a JSON string for stdout."""
    _key, top3_list = key_and_top3
    return json.dumps(top3_list)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("top3_product_views")

# Ingest events; TestingSource emits items as-is: (product_id, increment).
stream = op.input("product_events", flow, TestingSource(_events))

# The stream items are already (str, int) tuples – i.e. already keyed by
# product_id, which is what stateful_map expects: Stream[Tuple[str, V]].
counts_stream = op.stateful_map("count_per_product", stream, _count_mapper)
# counts_stream: Stream[Tuple[product_id, total_count]]

# Re-key every update to the single partition "global".
global_stream = op.map("reroute_global", counts_stream, _reroute_to_global)
# global_stream: Stream[Tuple["global", (product_id, total_count)]]

top3_stream = op.stateful_map("top3_global", global_stream, _topk_mapper)
# top3_stream: Stream[Tuple["global", [[pid, cnt], ...]]]

# Format and print.
formatted_stream = op.map("format_output", top3_stream, _format_output)
op.output("stdout_sink", formatted_stream, StdOutSink())
