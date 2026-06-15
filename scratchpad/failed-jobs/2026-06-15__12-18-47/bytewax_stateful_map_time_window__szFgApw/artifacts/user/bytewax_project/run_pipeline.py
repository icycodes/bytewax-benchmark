import json
from datetime import datetime
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def parse_json(line):
    return json.loads(line)

def key_on_user(event):
    return event["user_id"]

def parse_time(ts_str):
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)

def dedup_mapper(state, event):
    if state is None:
        state = {}

    current_time = parse_time(event["timestamp"])
    event_id = event["event_id"]

    # Prune old events
    keys_to_delete = []
    for eid, ts in state.items():
        if (current_time - ts).total_seconds() > 10:
            keys_to_delete.append(eid)
    
    for eid in keys_to_delete:
        del state[eid]

    # Check for duplicate
    is_duplicate = False
    if event_id in state:
        if (current_time - state[event_id]).total_seconds() <= 10:
            is_duplicate = True

    if not is_duplicate:
        state[event_id] = current_time
        emit_value = event
    else:
        emit_value = None

    return (state, emit_value)

def extract_and_format(key_event):
    key, event = key_event
    if event is None:
        return None
    # FileSink expects (key, string)
    return (key, json.dumps(event))

flow = Dataflow("dedup")

# 1. Read input events
lines = op.input("in", flow, FileSource("events.json"))

# 2. Parse JSON
events = op.map("parse", lines, parse_json)

# 3. Key the stream by user_id
keyed_events = op.key_on("key_on_user", events, key_on_user)

# 4. Stateful map for deduplication
deduped = op.stateful_map("dedup", keyed_events, dedup_mapper)

# 5. Filter out Nones and format to JSON string
json_out = op.filter_map("format", deduped, extract_and_format)

# 6. Write to output.json
op.output("out", json_out, FileSink("output.json"))

if __name__ == "__main__":
    run_main(flow)
