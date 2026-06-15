import json
import copy
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

# Read input events from input.json
with open("input.json") as f:
    events = json.load(f)

flow = Dataflow("top_k")

# Step 1: Read events from TestingSource
inp = op.input("in", flow, TestingSource(events))

# Step 2: Key each event by product_id
# Stream: ("prod_1", ["prod_1", 1])
keyed = op.key_on("key_prod", inp, key=lambda e: e[0])


# Step 3: Stateful map to maintain running count per product
def count_mapper(state, value):
    """Maintain running total for each product.

    Args:
        state: Running count (int) or None on first call.
        value: [product_id, increment] event.

    Returns:
        (new_state, (product_id, new_count))
    """
    product_id, increment = value
    if state is None:
        state = 0
    state = state + increment
    return (state, (product_id, state))


counted = op.stateful_map("count", keyed, count_mapper)

# Step 4: Remove product-level key, then re-key to "global"
# Stream: ("prod_1", ("prod_1", 5)) -> ("prod_1", 5) -> ("global", ("prod_1", 5))
unkeyed = op.key_rm("unkey", counted)
global_keyed = op.key_on("key_global", unkeyed, key=lambda _: "global")


# Step 5: Stateful map to maintain global Top-3 products
def top3_mapper(state, value):
    """Maintain global top-3 products by view count.

    Args:
        state: Dict of {product_id: total_count} or None on first call.
        value: (product_id, count) update from the count step.

    Returns:
        (new_state, json_string) where json_string is the current Top-3
        as a JSON-encoded list of [product_id, count] arrays.
    """
    product_id, count = value
    if state is None:
        state = {}
    else:
        # Deep copy to avoid silent state corruption during snapshotting
        state = copy.deepcopy(state)
    state[product_id] = count

    # Compute top 3 sorted by count descending
    sorted_items = sorted(state.items(), key=lambda x: x[1], reverse=True)[:3]
    top3 = [[k, v] for k, v in sorted_items]

    return (state, json.dumps(top3))


top3 = op.stateful_map("top3", global_keyed, top3_mapper)

# Step 6: Remove the "global" key and output to stdout
out = op.key_rm("unkey_global", top3)
op.output("out", out, StdOutSink())