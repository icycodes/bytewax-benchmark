import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Tuple

from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic
from bytewax.operators import input as bytewax_input
from bytewax.operators import key_on, map, map_value, output, stateful
from bytewax.testing import run_main


class DedupLogic(StatefulLogic):
    """Stateful deduplication logic.

    An event is emitted only if its event_id has not been seen for the
    same user_id within the last 10 seconds. State maps event_id to
    the timestamp of its last emitted occurrence. Entries older than
    10 seconds from the current event's timestamp are cleaned up.
    """

    def __init__(self, state: Optional[Dict[str, datetime]]) -> None:
        self._state: Dict[str, datetime] = state if state is not None else {}

    def on_item(self, value: dict) -> Tuple[Iterable[dict], bool]:
        event_id = value["event_id"]
        event_ts = value["_parsed_ts"]
        last_ts = self._state.get(event_id)

        # Clean up stale entries: remove any event_id whose last
        # timestamp is older than 10 seconds from the current event.
        cutoff = event_ts - timedelta(seconds=10)
        stale_ids = [
            eid for eid, ts in self._state.items() if ts <= cutoff
        ]
        for eid in stale_ids:
            del self._state[eid]

        # Decide whether to emit.
        emit = False
        if last_ts is None:
            # First time seeing this event_id — emit.
            emit = True
        else:
            diff = (event_ts - last_ts).total_seconds()
            if diff > 10.0:
                # More than 10 seconds since last emission — emit.
                emit = True
            # else: within 10 seconds (inclusive) — drop.

        if emit:
            self._state[event_id] = event_ts
            return ([value], StatefulLogic.RETAIN)
        else:
            return ([], StatefulLogic.RETAIN)

    def snapshot(self) -> Dict[str, datetime]:
        return dict(self._state)


def parse_json(line: str) -> dict:
    """Parse a JSON line into a dict and parse the timestamp."""
    obj = json.loads(line)
    obj["_parsed_ts"] = datetime.fromisoformat(
        obj["timestamp"].replace("Z", "+00:00")
    )
    return obj


def format_output(value: dict) -> str:
    """Format the event dict back to a JSON string, removing internal fields."""
    out = {
        "user_id": value["user_id"],
        "event_id": value["event_id"],
        "timestamp": value["timestamp"],
    }
    return json.dumps(out)


def build_dedup_state(resume_state: Optional[Dict[str, datetime]]) -> DedupLogic:
    """Builder function for the stateful operator."""
    return DedupLogic(resume_state)


def main():
    flow = Dataflow("dedup_flow")

    # Read lines from input.jsonl
    lines = bytewax_input("read", flow, FileSource("/home/user/myproject/input.jsonl"))

    # Parse JSON strings into dicts with parsed timestamps
    events = map("parse", lines, parse_json)

    # Key by user_id so state is per-user
    keyed = key_on("key_on_user", events, lambda e: e["user_id"])

    # Apply stateful deduplication
    deduped = stateful("dedup", keyed, build_dedup_state)

    # Strip the key and format back to JSON strings
    formatted = map_value("format", deduped, format_output)

    # Write to output.jsonl
    output("write", formatted, FileSink("/home/user/myproject/output.jsonl"))

    run_main(flow)


if __name__ == "__main__":
    main()
