import json
import sqlite3
from datetime import timedelta
from typing import List, Optional

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class EventsPartition(StatefulSourcePartition):
    """Reads events from SQLite in batches of up to 5 records."""

    DB_PATH = "/home/user/app/events.db"
    BATCH_SIZE = 5

    def __init__(self, resume_state: Optional[int]) -> None:
        # Connect here (not in the source) to avoid pickling issues during recovery.
        self._conn = sqlite3.connect(self.DB_PATH)
        self._conn.row_factory = sqlite3.Row
        self._last_id: int = resume_state if resume_state is not None else 0

    def next_batch(self) -> List[dict]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT id, user_id, event_type, payload "
            "FROM events "
            "WHERE id > ? "
            "ORDER BY id ASC "
            "LIMIT ?",
            (self._last_id, self.BATCH_SIZE),
        )
        rows = cursor.fetchall()

        if not rows:
            raise StopIteration

        records = [dict(row) for row in rows]
        self._last_id = records[-1]["id"]
        return records

    def snapshot(self) -> int:
        """Return the last processed id for exactly-once recovery."""
        return self._last_id

    def close(self) -> None:
        self._conn.close()


class EventsSource(FixedPartitionedSource):
    """Single-partition source backed by the SQLite events table."""

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> EventsPartition:
        return EventsPartition(resume_state)


def format_batch(user_id_and_events: tuple) -> str:
    """Convert a (user_id, [event, ...]) tuple to a JSON string."""
    user_id, events = user_id_and_events
    return json.dumps({"user_id": user_id, "events": events})


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("batching_flow")

# 1. Ingest events from SQLite.
events_stream = op.input("events_input", flow, EventsSource())

# 2. Key each event by user_id so downstream stateful ops are per-user.
keyed_stream = op.key_on("key_by_user", events_stream, lambda e: e["user_id"])

# 3. Micro-batch: collect up to 3 events per user or flush after 1 second.
batched_stream = op.collect(
    "micro_batch",
    keyed_stream,
    timedelta(seconds=1),
    max_size=3,
)

# 4. Serialize each batch to a JSON string.
json_stream = op.map("to_json", batched_stream, format_batch)

# 5. Write to stdout.
op.output("stdout_output", json_stream, StdOutSink())
