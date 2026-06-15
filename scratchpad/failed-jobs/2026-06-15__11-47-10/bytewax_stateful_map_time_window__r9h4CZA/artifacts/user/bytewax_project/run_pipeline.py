import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main


def parse_json(line: str) -> Optional[dict]:
    """Parse a line of text as JSON."""
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def deduplicate_mapper(
    state: Optional[dict], event: dict
) -> Tuple[Optional[dict], Optional[dict]]:
    """Stateful mapper to deduplicate events based on timestamp window."""
    if state is None:
        state = {}

    current_dt = datetime.fromisoformat(event["timestamp"])
    event_id = event["event_id"]

    # Prune old events from state (remove any event_ids older than 10 seconds compared to current event's timestamp)
    pruned_state = {}
    for eid, seen_dt in state.items():
        if current_dt - seen_dt <= timedelta(seconds=10):
            pruned_state[eid] = seen_dt
    state = pruned_state

    # Check if event_id is already seen within the last 10 seconds (i.e. still in pruned state)
    if event_id in state:
        # Duplicate! Do not emit, keep state as is
        return (state, None)
    else:
        # New event! Add to state and emit
        state[event_id] = current_dt
        return (state, event)


def main():
    # Define the dataflow
    flow = Dataflow("event-deduplication")

    # 1. Read input events from events.json
    input_stream = op.input("read_input", flow, FileSource("events.json"))

    # 2. Parse JSON lines
    parsed_stream = op.filter_map("parse_json", input_stream, parse_json)

    # 3. Key the stream by user_id
    keyed_stream = op.key_on(
        "key_by_user", parsed_stream, lambda event: event["user_id"]
    )

    # 4. Use stateful_map to maintain state and deduplicate
    stateful_stream = op.stateful_map("deduplicate", keyed_stream, deduplicate_mapper)

    # 5. Filter out None values (duplicates)
    deduped_stream = op.filter_map_value(
        "filter_duplicates", stateful_stream, lambda val: val
    )

    # 6. Format the output back to JSON string
    formatted_stream = op.map_value("format_output", deduped_stream, json.dumps)

    # 7. Write the downstream events to output.json
    op.output("write_output", formatted_stream, FileSink(Path("output.json")))

    # Run the dataflow
    run_main(flow)


if __name__ == "__main__":
    main()
