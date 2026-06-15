import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators import StatefulLogic
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main


INPUT_PATH = Path("/home/user/myproject/input.jsonl")
OUTPUT_PATH = Path("/home/user/myproject/output.jsonl")


def parse_line(line: str) -> dict:
    """Parse a JSON line into an event dict with parsed timestamp."""
    event = json.loads(line.strip())
    event["_ts"] = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    return event


class DedupLogic(StatefulLogic[dict, dict, Dict[str, datetime]]):
    """Stateful deduplication logic.

    State is a dict mapping event_id -> datetime of its last emitted occurrence.
    An event is emitted only if its event_id has not been seen within the last
    10 seconds (inclusive). If more than 10 seconds have passed since the last
    occurrence, it is emitted and the timestamp is updated.

    On each item, stale entries (older than 10s from the current event's
    timestamp) are cleaned up from the state.
    """

    def __init__(self, initial_state: Optional[Dict[str, datetime]] = None):
        self._state: Dict[str, datetime] = initial_state if initial_state is not None else {}

    def on_item(self, value: dict) -> Tuple[Iterable[dict], bool]:
        event_id = value["event_id"]
        current_ts = value["_ts"]
        window = timedelta(seconds=10)

        # Clean up stale entries: remove any event_id whose last seen timestamp
        # is older than 10 seconds from the current event's timestamp.
        stale_ids = [
            eid for eid, ts in self._state.items()
            if current_ts - ts > window
        ]
        for eid in stale_ids:
            del self._state[eid]

        # Check if this event_id is a duplicate within the 10-second window.
        last_ts = self._state.get(event_id)
        if last_ts is not None and current_ts <= last_ts + window:
            # Duplicate within the window — drop it.
            return ([], False)

        # Not a duplicate — emit it and update state.
        self._state[event_id] = current_ts
        return ([value], False)

    def snapshot(self) -> Dict[str, datetime]:
        return dict(self._state)


def builder(resume_state: Optional[Dict[str, datetime]]) -> DedupLogic:
    return DedupLogic(resume_state)


def format_output(keyed_event: Tuple[str, dict]) -> str:
    """Convert a (key, event_dict) back to a JSON string."""
    _, event = keyed_event
    # Remove the internal _ts field before serializing
    out = {k: v for k, v in event.items() if k != "_ts"}
    return json.dumps(out)


def build_dataflow() -> Dataflow:
    flow = Dataflow("dedup")

    # 1. Read lines from the input file.
    lines = op.input("input", flow, FileSource(INPUT_PATH))

    # 2. Parse JSON strings into event dicts.
    events = op.map("parse", lines, parse_line)

    # 3. Key the stream by user_id.
    keyed = op.key_on("key_on_user", events, lambda e: e["user_id"])

    # 4. Apply stateful deduplication.
    deduped = op.stateful("dedup", keyed, builder)

    # 5. Format back to JSON strings.
    json_lines = op.map("format", deduped, format_output)

    # 6. Key the output so it can be routed by the FileSink.
    keyed_output = op.key_on("key_output", json_lines, lambda _: "0")

    # 7. Write to output file.
    op.output("output", keyed_output, FileSink(OUTPUT_PATH))

    return flow


if __name__ == "__main__":
    flow = build_dataflow()
    run_main(flow)
