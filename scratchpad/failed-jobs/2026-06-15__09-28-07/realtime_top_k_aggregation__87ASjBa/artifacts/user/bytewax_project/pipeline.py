import json
import os
from typing import Optional, Dict, Tuple, List
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
import bytewax.operators as op

os.makedirs("./recovery_dir", exist_ok=True)

def read_input():
    if os.path.exists("input.json"):
        with open("input.json", "r") as f:
            return json.load(f)
    return []

flow = Dataflow("flow")

input_data = read_input()
stream = op.input("input", flow, TestingSource(input_data))

# Format input: events are [product_id, increment]
keyed_stream = op.map("key_by_product", stream, lambda x: (str(x[0]), x[1]))

def update_count(state: Optional[int], value: int) -> Tuple[int, int]:
    if state is None:
        state = 0
    new_state = state + value
    return new_state, new_state

count_stream = op.stateful_map("running_count", keyed_stream, update_count)

# count_stream emits (product_id, total_count)
global_stream = op.map("key_global", count_stream, lambda x: ("global", x))

def update_top3(state: Optional[Dict[str, int]], value: Tuple[str, int]) -> Tuple[Dict[str, int], Optional[List[List]]]:
    if state is None:
        state = {}
    
    product_id, count = value
    new_state = state.copy()
    new_state[product_id] = count
    
    # Sort and keep top 3
    sorted_items = sorted(new_state.items(), key=lambda x: x[1], reverse=True)[:3]
    
    # Rebuild state with only top 3
    final_state = {k: v for k, v in sorted_items}
    
    # Format output
    output = [[k, v] for k, v in sorted_items]
    
    if final_state == state:
        return final_state, None
    
    return final_state, output

top3_stream = op.stateful_map("top3", global_stream, update_top3)

# Filter out None values
filtered_top3_stream = op.filter("filter_none", top3_stream, lambda x: x[1] is not None)

# filtered_top3_stream emits ("global", [[product_id, count], ...])
def print_output(step_id, item):
    key, top3 = item
    print(json.dumps(top3))

op.inspect("print_output", filtered_top3_stream, print_output)
