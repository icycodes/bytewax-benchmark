import sqlite3
import json
from datetime import timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.stdio import StdOutSink

class SQLitePartition(StatefulSourcePartition):
    def __init__(self, db_path, resume_state):
        self.db_path = db_path
        self.last_id = resume_state if resume_state is not None else 0
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def next_batch(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, user_id, event_type, payload FROM events WHERE id > ? ORDER BY id ASC LIMIT 5",
            (self.last_id,)
        )
        rows = cursor.fetchall()
        if not rows:
            raise StopIteration()
        
        batch = []
        for row in rows:
            event = dict(row)
            self.last_id = event["id"]
            batch.append(event)
            
        return batch

    def snapshot(self):
        return self.last_id

    def close(self):
        self.conn.close()

class SQLiteSource(FixedPartitionedSource):
    def __init__(self, db_path):
        self.db_path = db_path

    def list_parts(self):
        return ["single"]

    def build_part(self, step_id, for_key, resume_state):
        return SQLitePartition(self.db_path, resume_state)

def format_output(item):
    user_id, events = item
    return json.dumps({"user_id": user_id, "events": events})

flow = Dataflow("batching_flow")

# Read from SQLite
stream = op.input("read_sqlite", flow, SQLiteSource("/home/user/app/events.db"))

# Key the stream by user_id
keyed_stream = op.key_on("key_by_user", stream, lambda event: event["user_id"])

# Micro-batch events
batched_stream = op.collect(
    "micro_batch", 
    keyed_stream, 
    timeout=timedelta(seconds=1), 
    max_size=3
)

# Format output
formatted_stream = op.map("format_json", batched_stream, format_output)

# Write to standard output
op.output("write_stdout", formatted_stream, StdOutSink())
