import json
from datetime import datetime
from pathlib import Path
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main
from bytewax.operators import StatefulLogic

class DeduplicateLogic(StatefulLogic):
    def __init__(self, resume_state):
        self.last_seen = resume_state or {}

    def on_item(self, value):
        event_id = value["event_id"]
        event_time_str = value["timestamp"]
        event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        
        # Cleanup old events
        to_remove = []
        for eid, last_time in self.last_seen.items():
            if (event_time - last_time).total_seconds() > 10:
                to_remove.append(eid)
        for eid in to_remove:
            del self.last_seen[eid]
            
        # Check deduplication
        if event_id in self.last_seen:
            last_time = self.last_seen[event_id]
            if (event_time - last_time).total_seconds() <= 10:
                return ([], False)
        
        self.last_seen[event_id] = event_time
        return ([value], False)

    def snapshot(self):
        return self.last_seen.copy()

def build_logic(resume_state):
    return DeduplicateLogic(resume_state)

flow = Dataflow("dedup")
inp = op.input("inp", flow, FileSource("input.jsonl"))
parsed = op.map("parse", inp, json.loads)
keyed = op.key_on("key", parsed, lambda x: x["user_id"])
deduped = op.stateful("dedup", keyed, build_logic)
out_strs = op.map("to_json", deduped, lambda x: (x[0], json.dumps(x[1])))
op.output("out", out_strs, FileSink(Path("output.jsonl")))

if __name__ == "__main__":
    run_main(flow)
