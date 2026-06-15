import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

# Define the dataflow
flow = Dataflow("rate_limiting_flow")

# Source input from input.jsonl
input_path = Path("/home/user/bytewax_project/input.jsonl")
stream = op.input("input", flow, FileSource(input_path))

# Parse JSON strings to dictionaries
def parse_json(line):
    try:
        return json.loads(line)
    except Exception:
        return None

parsed = op.filter_map("parse_json", stream, parse_json)

# Key the events by user_id as string
keyed = op.key_on("key_user", parsed, lambda event: str(event["user_id"]))

# Stateful rate limiter
def rate_limit(state, event):
    # state is (current_tokens, last_update_timestamp)
    # event is {"user_id": ..., "event_id": ..., "timestamp": ...}
    
    current_time = event["timestamp"]
    
    if state is None:
        current_tokens = 5.0
        elapsed_time = 0.0
    else:
        prev_tokens, last_update_timestamp = state
        elapsed_time = current_time - last_update_timestamp
        current_tokens = prev_tokens + elapsed_time * 1.0
        
    # Cap tokens at max capacity
    current_tokens = min(current_tokens, 5.0)
    
    # Check if we have enough tokens
    if current_tokens >= 1.0:
        current_tokens -= 1.0
        allowed = True
    else:
        allowed = False
        
    new_state = (current_tokens, current_time)
    emit_value = event if allowed else None
    
    return new_state, emit_value

# Apply rate limiting
rate_limited = op.stateful_map("rate_limit", keyed, rate_limit)

# Filter out None values (dropped events)
allowed_keyed = op.filter("filter_dropped", rate_limited, lambda key_val: key_val[1] is not None)

# Remove keys
allowed_events = op.key_rm("remove_keys", allowed_keyed)

# Convert back to JSON strings and key them
output_lines = op.map("to_json", allowed_events, lambda event: (str(event["user_id"]), json.dumps(event)))

# Write to output.jsonl
output_path = Path("/home/user/bytewax_project/output.jsonl")
op.output("write_output", output_lines, FileSink(output_path))
