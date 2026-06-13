import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

# Initialize the dataflow named dedup_flow
dedup_flow = Dataflow("dedup_flow")

# Define input and output paths
input_path = "/home/user/myproject/data/events.jsonl"
output_path = "/home/user/myproject/data/unique_events.jsonl"

# Read lines from input file
lines = op.input("input", dedup_flow, FileSource(input_path))

# Parse JSON lines safely
def parse_line(line):
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None

events = op.filter_map("parse_json", lines, parse_line)

# Partition state by user_id (cast non-string user IDs to string as required by Bytewax)
keyed_events = op.key_on("key_by_user", events, lambda event: str(event["user_id"]))

# Stateful map to deduplicate message_ids per user.
# The mapper receives (state, event) where state is a frozenset of seen message_ids.
# It returns (updated_state, emit_value).
def deduplicate(state, event):
    if state is None:
        state = frozenset()
    
    message_id = event["message_id"]
    if message_id in state:
        # Duplicate, return state unchanged and emit None
        return (state, None)
    else:
        # First time seeing this message_id for this user.
        # Create a new frozenset state and emit the event.
        new_state = state | frozenset([message_id])
        return (new_state, event)

deduped_keyed = op.stateful_map("deduplicate", keyed_events, deduplicate)

# Filter out duplicate events (which were mapped to None)
unique_keyed = op.filter_value("filter_duplicates", deduped_keyed, lambda event: event is not None)

# Convert unique events back to JSON strings while keeping the key for FileSink routing
formatted_lines = op.map_value("format_json", unique_keyed, lambda event: json.dumps(event))

# Write to the output file (FileSink requires a keyed stream of (key, line))
op.output("output", formatted_lines, FileSink(Path(output_path)))
