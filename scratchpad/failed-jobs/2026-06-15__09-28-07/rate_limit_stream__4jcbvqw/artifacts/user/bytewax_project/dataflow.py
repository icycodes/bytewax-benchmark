import json
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.files import FileSource, FileSink

class TokenBucketState:
    def __init__(self):
        self.tokens = 5.0
        self.last_update = None

    def process(self, event):
        timestamp = event["timestamp"]
        
        if self.last_update is None:
            self.last_update = timestamp
            self.tokens = 5.0
        else:
            elapsed = timestamp - self.last_update
            self.tokens = min(5.0, self.tokens + elapsed * 1.0)
            self.last_update = timestamp

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return self, event
        else:
            return self, None

def rate_limit_mapper(state, event):
    if state is None:
        state = TokenBucketState()
    new_state, result = state.process(event)
    return new_state, result

flow = Dataflow("rate_limit")
stream = op.input("input", flow, FileSource("/home/user/bytewax_project/input.jsonl"))

# Parse JSON
parsed = op.map("parse", stream, json.loads)

# Key by user_id
keyed = op.key_on("key_by_user", parsed, lambda e: str(e["user_id"]))

# Stateful map
processed = op.stateful_map("rate_limit", keyed, rate_limit_mapper)

# Filter out dropped events
allowed_keyed = op.filter_value("filter_dropped", processed, lambda v: v is not None)

# Format as JSON
formatted = op.map_value("format", allowed_keyed, json.dumps)

# Write to output
op.output("output", formatted, FileSink("/home/user/bytewax_project/output.jsonl"))
