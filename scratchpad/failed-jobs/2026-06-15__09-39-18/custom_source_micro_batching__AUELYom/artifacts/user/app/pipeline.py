import json
import sqlite3
from datetime import timedelta

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

DB_PATH = "/home/user/app/events.db"


class SQLitePartition(StatefulSourcePartition):
    """Reads events from SQLite in batches of up to 5 records."""

    def __init__(self, resume_state):
        # Establish the connection here to avoid pickling errors during recovery
        self._conn = sqlite3.connect(DB_PATH)
        self._conn.row_factory = sqlite3.Row
        self._last_id = resume_state if resume_state is not None else 0

    def next_batch(self):
        cur = self._conn.cursor()
        cur.execute(
            "SELECT id, user_id, event_type, payload FROM events WHERE id > ? ORDER BY id LIMIT 5",
            (self._last_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise StopIteration()
        batch = [
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "event_type": row["event_type"],
                "payload": row["payload"],
            }
            for row in rows
        ]
        self._last_id = rows[-1]["id"]
        return batch

    def snapshot(self):
        return self._last_id

    def close(self):
        self._conn.close()


class SQLiteSource(FixedPartitionedSource):
    """A fixed-partition source with a single partition for the SQLite events table."""

    def list_parts(self):
        return ["single"]

    def build_part(self, step_id, for_part, resume_state):
        return SQLitePartition(resume_state)


flow = Dataflow("batching_flow")

inp = op.input("sqlite_input", flow, SQLiteSource())
keyed = op.key_on("key_on_user", inp, lambda ev: ev["user_id"])
collected = op.collect("collect_events", keyed, timedelta(seconds=1), max_size=3)
json_out = op.map("to_json", collected, lambda item: json.dumps({"user_id": item[0], "events": item[1]}))
op.output("stdout", json_out, StdOutSink())