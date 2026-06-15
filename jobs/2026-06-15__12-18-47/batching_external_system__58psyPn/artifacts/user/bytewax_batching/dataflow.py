import os
import json
import sqlite3
from datetime import timedelta
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op

# Environment variables
run_id = os.environ.get("ZEALT_RUN_ID", "default")
db_path = f"/home/user/bytewax_batching/metrics-{run_id}.db"
input_path = "/home/user/bytewax_batching/input.jsonl"
log_path = "/home/user/bytewax_batching/output.log"

# Create the SQLite table before starting
def init_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS device_metrics (
            device_id TEXT,
            timestamp TEXT,
            metric_value REAL
        )
    ''')
    conn.commit()
    conn.close()

init_db()

class SQLitePartition(StatelessSinkPartition):
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def write_batch(self, items):
        # items is a List of Tuple[str, List[dict]]
        # We need to flatten it
        records = []
        for key, batch in items:
            for item in batch:
                records.append((item['device_id'], item['timestamp'], item['metric_value']))
        if records:
            self.cursor.executemany('''
                INSERT INTO device_metrics (device_id, timestamp, metric_value)
                VALUES (?, ?, ?)
            ''', records)
            self.conn.commit()

    def close(self):
        self.conn.close()

class SQLiteSink(DynamicSink):
    def __init__(self, db_path):
        self.db_path = db_path

    def build(self, step_id, worker_index, worker_count):
        return SQLitePartition(self.db_path)

flow = Dataflow("batching_pipeline")

# Read from file
lines = op.input("input", flow, FileSource(input_path))

# Parse JSON
def parse_json(line):
    return json.loads(line)

parsed = op.map("parse", lines, parse_json)

# Filter out metric_value < 0
def filter_metrics(item):
    return item.get("metric_value", 0) >= 0

filtered = op.filter("filter", parsed, filter_metrics)

# Group by device_id
def key_by_device(item):
    return item["device_id"], item

keyed = op.map("key", filtered, key_by_device)

# Collect into batches
# max_size=10, timeout=1 second
batched = op.collect("batch", keyed, timeout=timedelta(seconds=1), max_size=10)

# Write to SQLite
op.output("out", batched, SQLiteSink(db_path))

if __name__ == "__main__":
    run_main(flow)
    with open(log_path, "a") as f:
        f.write("Pipeline finished.\n")
