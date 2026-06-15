import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic

class Deduplicator(StatefulLogic):
    def __init__(self, resume_state: Optional[dict]):
        self.state = resume_state if resume_state is not None else {}

    def on_item(self, value: dict):
        event_id = value['event_id']
        timestamp_str = value['timestamp']
        # Parse ISO 8601 timestamp
        current_ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        
        # Cleanup state: remove event_ids older than 10 seconds from current_ts
        keys_to_delete = []
        for e_id, last_ts in self.state.items():
            if (current_ts - last_ts).total_seconds() > 10:
                keys_to_delete.append(e_id)
        for e_id in keys_to_delete:
            del self.state[e_id]
            
        emit = False
        if event_id not in self.state:
            emit = True
            self.state[event_id] = current_ts
        else:
            last_ts = self.state[event_id]
            if (current_ts - last_ts).total_seconds() > 10:
                emit = True
                self.state[event_id] = current_ts
            else:
                emit = False
        
        if emit:
            return [value], False
        else:
            return [], False

    def snapshot(self) -> dict:
        return self.state

def build_flow():
    flow = Dataflow("deduplication")
    
    # Read from file
    stream = op.input("input", flow, FileSource("input.jsonl"))
    
    # Parse JSON
    stream = op.map("parse_json", stream, json.loads)
    
    # Key by user_id
    keyed_stream = op.key_on("key_by_user", stream, lambda e: e["user_id"])
    
    # Stateful deduplication
    processed = op.stateful("dedup", keyed_stream, lambda resume_state: Deduplicator(resume_state))
    
    # Format to JSON string for output
    formatted = op.map("format_json", processed, lambda kv: (kv[0], json.dumps(kv[1])))
    
    # Write to output file
    op.output("output", formatted, FileSink(Path("output.jsonl")))
    
    return flow

if __name__ == "__main__":
    from bytewax.testing import run_main
    flow = build_flow()
    run_main(flow)
