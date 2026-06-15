import json
import sqlite3
from datetime import timedelta
from typing import Iterable, List, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.stdio import StdOutSink

DB_PATH = "/home/user/app/events.db"


class SQLitePartition(StatefulSourcePartition):
    """Partition that reads from SQLite in batches, maintaining resume state."""

    def __init__(self, resume_state: Optional[int]):
        self._conn = sqlite3.connect(DB_PATH)
        self._conn.row_factory = sqlite3.Row
        self._last_id = resume_state if resume_state is not None else 0

    def next_batch(self) -> List[dict]:
        cursor = self._conn.execute(
            "SELECT id, user_id, event_type, payload FROM events WHERE id > ? ORDER BY id ASC LIMIT 5",
            (self._last_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise StopIteration
        results = [dict(row) for row in rows]
        self._last_id = results[-1]["id"]
        return results

    def snapshot(self) -> Optional[int]:
        return self._last_id if self._last_id != 0 else None

    def close(self) -> None:
        self._conn.close()


class SQLiteSource(FixedPartitionedSource):
    """Fixed-partition source that reads from SQLite, one partition."""

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(self, step_id: str, for_part: str, resume_state: Optional[int]) -> SQLitePartition:
        return SQLitePartition(resume_state)


flow = Dataflow("batching_flow")

# Input: read from SQLite in batches of up to 5
inp = op.input("db_input", flow, SQLiteSource())

# Key by user_id
keyed = op.key_on("key_by_user", inp, lambda event: event["user_id"])

# Micro-batch: collect up to 3 events per user, or flush after 1 second
collected = op.collect("micro_batch", keyed, timeout=timedelta(seconds=1), max_size=3)

# Map to JSON string: {"user_id": "...", "events": [...]}
def format_batch(item):
    user_id, events = item
    return json.dumps({"user_id": user_id, "events": events})

formatted = op.map("format_json", collected, format_batch)

# Output to stdout
op.output("stdout_sink", formatted, StdOutSink())
