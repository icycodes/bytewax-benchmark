import json

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import TestingSource


flow = Dataflow("top3_aggregation")


def load_events():
    """Load events from input.json."""
    with open("input.json", "r") as f:
        events = json.load(f)
    return events


def count_mapper(current_count, event):
    """Maintain running count for a single product.

    `event` is the original list [product_id, increment].

    Returns (new_state, emit_value) where emit_value is (product_id, new_count).
    """
    product_id, increment = event
    if current_count is None:
        current_count = 0
    new_count = current_count + increment
    return (new_count, (product_id, new_count))


def top3_mapper(state, event):
    """Maintain the global Top-3 list.

    `event` is a tuple of (product_id, new_count).

    Returns (new_state, emit_value) where emit_value is the new Top-3 list
    if it changed, otherwise None.
    """
    if state is None:
        state = {}  # dict of product_id -> count

    product_id, new_count = event

    # Create a new copy of the state to avoid silent state corruption
    # during Bytewax's snapshotting process
    new_state = dict(state)
    new_state[product_id] = new_count

    # Compute the new Top-3: sort by count descending, take top 3
    new_top3 = sorted(new_state.items(), key=lambda x: x[1], reverse=True)[:3]

    # Convert to list of lists for JSON output
    new_top3_list = [[pid, cnt] for pid, cnt in new_top3]

    return (new_state, new_top3_list)


def emit_only_changes(old_top3, new_top3):
    """Filter: only emit when the Top-3 list has changed."""
    if old_top3 is None:
        old_top3 = []
    if old_top3 == new_top3:
        return (old_top3, None)
    return (new_top3, new_top3)


# Step 1: Read events from input.json
events = load_events()
inp = op.input("input", flow, TestingSource(events))

# Step 2: Key each event by product_id (first element of the list)
# Each event is [product_id, increment]
keyed = op.key_on("key_on_product", inp, lambda x: x[0])

# Step 3: Maintain running count per product
# The mapper receives (current_count, event) where event is [product_id, increment]
counted = op.stateful_map("count_per_product", keyed, count_mapper)

# Now counted is a stream of (product_id, (product_id, new_count))
# Extract just the (product_id, new_count) for downstream processing
def extract_count(kv):
    """Extract (product_id, new_count) from (product_id, (product_id, new_count))."""
    return kv[1]

product_counts = op.map("extract_count", counted, extract_count)

# Re-key everything to a single "global" key for the top-3 aggregation
global_keyed = op.key_on("key_on_global", product_counts, lambda x: "global")

# Step 4: Maintain global Top-3 list
# The mapper receives (state_dict, (product_id, new_count))
top3_stream = op.stateful_map("top3_global", global_keyed, top3_mapper)

# Step 5: Filter to only emit when the Top-3 list actually changes
top3_filtered = op.stateful_map("dedup_top3", top3_stream, emit_only_changes)

# Step 6: Filter out None values (no-change events) and extract just the top-3 list
def extract_top3_list(kv):
    """Extract the top-3 list from the keyed tuple, or return None if value is None."""
    _, value = kv
    return value if value is not None else None

top3_changes = op.filter_map("filter_and_extract", top3_filtered, extract_top3_list)

# Step 7: Format as JSON and output to stdout
top3_json = op.map("to_json", top3_changes, json.dumps)

op.output("stdout", top3_json, StdOutSink())
