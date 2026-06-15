import json
import sqlite3
from datetime import timedelta
from typing import List, Optional, Iterable, Dict, Any

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.stdio import StdOutSink
import bytewax.operators as op


class SQLitePartition(StatefulSourcePartition[Dict[str, Any], Optional[int]]):
    def __init__(self, db_path: str, resume_state: Optional[int]):
        self._db_path = db_path
        self._last_id = resume_state if resume_state is not None else 0
        self._conn = sqlite3.connect(self._db_path)

    def next_batch(self) -> Iterable[Dict[str, Any]]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT id, user_id, event_type, payload FROM events WHERE id > ? ORDER BY id ASC LIMIT 5",
            (self._last_id,)
        )
        rows = cursor.fetchall()
        if not rows:
            raise StopIteration

        batch = []
        for row in rows:
            item = {
                "id": row[0],
                "user_id": row[1],
                "event_type": row[2],
                "payload": row[3],
            }
            batch.append(item)

        self._last_id = rows[-1][0]
        return batch

    def snapshot(self) -> Optional[int]:
        return self._last_id

    def close(self) -> None:
        if self._conn:
            self._conn.close()


class SQLiteSource(FixedPartitionedSource[Dict[str, Any], Optional[int]]):
    def __init__(self, db_path: str):
        self._db_path = db_path

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> SQLitePartition:
        return SQLitePartition(self._db_path, resume_state)


# Create the Dataflow named batching_flow assigned to the variable flow
flow = Dataflow("batching_flow")

# Add the SQLite source as input
stream = op.input("sqlite_input", flow, SQLiteSource("/home/user/app/events.db"))

# Key the stream by user_id
keyed_stream = op.key_on("key_by_user", stream, lambda x: x["user_id"])

# Use the op.collect operator to micro-batch events for each user.
# Set the timeout to 1 second and max_size to 3.
collected_stream = op.collect(
    "collect_batches",
    keyed_stream,
    timeout=timedelta(seconds=1),
    max_size=3
)

# Map the output of collect to a JSON string with the format:
# {"user_id": "<user_id>", "events": [<list_of_event_dicts>]}
def format_batch(item):
    user_id, events = item
    return json.dumps({
        "user_id": user_id,
        "events": events
    })

formatted_stream = op.map("format_batch", collected_stream, format_batch)

# Write the JSON strings to standard output using StdOutSink
op.output("stdout_output", formatted_stream, StdOutSink())
