import json
import datetime
from pathlib import Path
from typing import Tuple, Iterable, Dict, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic


class DeduplicateLogic(StatefulLogic[dict, dict, dict]):
    """
    Stateful logic for deduplicating events per user_id.
    Keeps track of seen event_ids and their last emitted timestamp.
    Cleans up any event_id older than 10 seconds from the current event's timestamp.
    """
    def __init__(self, state: Optional[dict]):
        if state is None:
            self.state: Dict[str, datetime.datetime] = {}
        else:
            self.state = state

    def on_item(self, event: dict) -> Tuple[Iterable[dict], bool]:
        current_ts = datetime.datetime.fromisoformat(event["timestamp"])
        event_id = event["event_id"]

        # 1. Dynamic Cleanup: remove any event_id older than 10 seconds from the current event's timestamp
        to_delete = []
        for eid, last_ts in self.state.items():
            if current_ts - last_ts > datetime.timedelta(seconds=10):
                to_delete.append(eid)
        for eid in to_delete:
            del self.state[eid]

        # 2. Check and deduplicate
        if event_id in self.state:
            # The event is a duplicate within the 10-second window.
            # Do not emit, and keep the previous timestamp (do not update).
            # Discard logic if the state is empty (to prevent memory leaks for inactive users).
            return ([], len(self.state) == 0)
        else:
            # First time seeing this event_id or it was cleaned up because it is older than 10 seconds.
            # Emit the event and record the timestamp.
            self.state[event_id] = current_ts
            return ([event], len(self.state) == 0)

    def snapshot(self) -> dict:
        # Return a copy of the state for recovery
        return self.state.copy()


def parse_json(line: str) -> dict:
    return json.loads(line)


def key_by_user(event: dict) -> Tuple[str, dict]:
    return (event["user_id"], event)


def format_output(item: Tuple[str, dict]) -> Tuple[str, str]:
    user_id, event = item
    return (user_id, json.dumps(event))


# Build the Bytewax dataflow
flow = Dataflow("deduplication_flow")

# 1. Read lines from the input file
input_stream = op.input("input_step", flow, FileSource(Path("/home/user/myproject/input.jsonl")))

# 2. Parse JSON lines into dictionaries
parsed_stream = op.map("parse_json", input_stream, parse_json)

# 3. Map to a keyed stream: (user_id, event_dict)
keyed_stream = op.map("key_by_user", parsed_stream, key_by_user)

# 4. Apply stateful deduplication logic
deduped_stream = op.stateful("deduplicate", keyed_stream, DeduplicateLogic)

# 5. Format the output back to JSON strings
output_stream = op.map("format_output", deduped_stream, format_output)

# 6. Write the output to the output file
op.output("output_step", output_stream, FileSink(Path("/home/user/myproject/output.jsonl")))


if __name__ == "__main__":
    from bytewax.testing import run_main
    run_main(flow)
