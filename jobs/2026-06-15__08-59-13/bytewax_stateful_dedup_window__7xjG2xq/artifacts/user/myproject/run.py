import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic
from bytewax.testing import run_main


def parse_json(line: str) -> dict:
    data = json.loads(line)
    ts_str = data["timestamp"]
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    data["timestamp"] = datetime.fromisoformat(ts_str)
    return data


def key_by_user(event: dict) -> Tuple[str, dict]:
    return (event["user_id"], event)


class DeduplicatorLogic(StatefulLogic):
    def __init__(self, state: Optional[dict]):
        if state is None:
            self.state = {}
        else:
            self.state = state

    def on_item(self, value: dict) -> Tuple[Iterable[dict], bool]:
        event_id = value["event_id"]
        current_ts = value["timestamp"]

        # Clean up state: remove any event_id that is older than 10 seconds
        # from the current event's timestamp.
        to_remove = []
        for eid, prev_ts in self.state.items():
            if (current_ts - prev_ts).total_seconds() > 10.0:
                to_remove.append(eid)
        for eid in to_remove:
            del self.state[eid]

        # Check if the same event_id was seen within 10 seconds (inclusive)
        if event_id in self.state:
            # Drop the event
            return ([], StatefulLogic.RETAIN)
        else:
            # Emit the event and restart the 10-second window
            self.state[event_id] = current_ts
            return ([value], StatefulLogic.RETAIN)

    def snapshot(self) -> dict:
        return self.state.copy()


def format_json(value: dict) -> str:
    out_dict = value.copy()
    ts = out_dict["timestamp"]
    ts_str = ts.isoformat()
    if ts_str.endswith("+00:00"):
        ts_str = ts_str[:-6] + "Z"
    if "." in ts_str:
        base, frac = ts_str[:-1].split(".")
        frac = frac.rstrip("0")
        if frac:
            ts_str = f"{base}.{frac}Z"
        else:
            ts_str = f"{base}Z"
    out_dict["timestamp"] = ts_str
    return json.dumps(out_dict)


def main():
    flow = Dataflow("deduplication")
    
    # Read lines from input.jsonl
    input_path = Path("/home/user/myproject/input.jsonl")
    lines = op.input("input", flow, FileSource(input_path, batch_size=1))
    
    # Parse JSON and parse ISO 8601 timestamps into datetime objects
    parsed = op.map("parse_json", lines, parse_json)
    
    # Group events by user_id: key the stream as (user_id, event_dict)
    keyed = op.map("key_by_user", parsed, key_by_user)
    
    # Stateful deduplication logic
    deduplicated = op.stateful("dedup", keyed, DeduplicatorLogic)
    
    # Format back to JSON strings
    json_strings = op.map_value("format_json", deduplicated, format_json)
    
    # Write to output.jsonl
    output_path = Path("/home/user/myproject/output.jsonl")
    op.output("output", json_strings, FileSink(output_path))
    
    # Run the dataflow
    run_main(flow)


if __name__ == "__main__":
    main()
