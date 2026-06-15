import json
import os
import copy
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

# Create the Bytewax Dataflow
flow = Dataflow("top_k_aggregation")

# Load events from input.json
input_path = "input.json"
if os.path.exists(input_path):
    with open(input_path, "r") as f:
        events = json.load(f)
else:
    events = []

# Convert events to a list of tuples (product_id, increment)
# Each event in input.json is a list [product_id, increment]
events_tuples = [(event[0], event[1]) for event in events]

# Input stream
stream = op.input("inp", flow, TestingSource(events_tuples))

# Stateful map 1: Maintain the running count per product
def update_count(running_count, increment):
    if running_count is None:
        running_count = 0
    running_count += increment
    return (running_count, running_count)

count_stream = op.stateful_map("product_counts", stream, update_count)

# Map to route all product counts to a single stateful operator with a constant key "global"
def rekey_global(item):
    product_id, running_count = item
    return ("global", (product_id, running_count))

global_stream = op.map("rekey_global", count_stream, rekey_global)

# Stateful map 2: Maintain the global Top-3 products
def update_global_top_3(state, value):
    # state is a tuple: (product_counts, last_top_3)
    if state is None:
        product_counts = {}
        last_top_3 = []
    else:
        # Deep copy to avoid silent state corruption during snapshotting
        product_counts, last_top_3 = copy.deepcopy(state)
    
    product_id, running_count = value
    product_counts[product_id] = running_count
    
    # Compute the new Top-3 products sorted by count in descending order
    # Tie-breaking deterministically by product_id ascending
    sorted_products = sorted(product_counts.items(), key=lambda x: (-x[1], x[0]))
    new_top_3 = [[prod, count] for prod, count in sorted_products[:3]]
    
    if new_top_3 != last_top_3:
        # Top-3 has changed or updated
        new_state = (product_counts, new_top_3)
        return (new_state, new_top_3)
    else:
        # Top-3 has not changed
        new_state = (product_counts, last_top_3)
        return (new_state, None)

top_3_stream = op.stateful_map("global_top_3", global_stream, update_global_top_3)

# Filter out None values
filtered_stream = op.filter_map_value("filter_none", top_3_stream, lambda x: x)

# Discard the routing key "global"
value_stream = op.key_rm("remove_key", filtered_stream)

# Format the output as a JSON-encoded string
formatted_stream = op.map("format_json", value_stream, lambda x: json.dumps(x))

# Output to standard output
op.output("out", formatted_stream, StdOutSink())
