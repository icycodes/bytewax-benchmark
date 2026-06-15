import json
import os
import sqlite3
from datetime import timedelta

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main

# Read the current run-id from the ZEALT_RUN_ID environment variable
run_id = os.environ.get("ZEALT_RUN_ID", "default")
db_path = f"/home/user/bytewax_batching/metrics-{run_id}.db"
input_path = "/home/user/bytewax_batching/input.jsonl"
log_path = "/home/user/bytewax_batching/output.log"

# Ensure the SQLite table is created before the dataflow starts writing
print(f"Initializing SQLite database at {db_path}...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS device_metrics (
    device_id TEXT,
    timestamp TEXT,
    metric_value REAL
)
""")
conn.commit()
conn.close()

# Custom Input Source to read device metrics from a JSONL file
class JSONLSourcePartition(StatelessSourcePartition):
    def __init__(self, filepath, worker_index, worker_count):
        self.filepath = filepath
        self.worker_index = worker_index
        self.worker_count = worker_count
        self._f = open(filepath, "r")
        self._completed = False
        self.line_idx = 0

    def next_batch(self):
        if self._completed:
            raise StopIteration()

        batch = []
        while True:
            line = self._f.readline()
            if not line:
                break

            i = self.line_idx
            self.line_idx += 1

            if i % self.worker_count == self.worker_index:
                try:
                    data = json.loads(line.strip())
                    batch.append(data)
                except Exception as e:
                    print(f"Error parsing JSON line {i}: {e}")
                
                # Yield a small batch to allow cooperative multi-tasking and timeout/batching operators
                if len(batch) >= 10:
                    return batch

        self._f.close()
        self._completed = True
        if batch:
            return batch
        raise StopIteration()

    def close(self):
        try:
            self._f.close()
        except Exception:
            pass


class JSONLSource(DynamicSource):
    def __init__(self, filepath):
        self.filepath = filepath

    def build(self, step_id: str, worker_index: int, worker_count: int) -> JSONLSourcePartition:
        return JSONLSourcePartition(self.filepath, worker_index, worker_count)


# Custom Output Sink to write the batched metrics to the SQLite database
class SQLiteSinkPartition(StatelessSinkPartition):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def write_batch(self, items):
        # items is a list of (device_id, list_of_metrics) tuples
        records_to_insert = []
        for device_id, metrics in items:
            for metric in metrics:
                records_to_insert.append((
                    metric.get("device_id"),
                    metric.get("timestamp"),
                    metric.get("metric_value")
                ))

        if records_to_insert:
            print(f"Performing bulk insert of {len(records_to_insert)} metrics...")
            self.cursor.executemany(
                "INSERT INTO device_metrics (device_id, timestamp, metric_value) VALUES (?, ?, ?)",
                records_to_insert
            )
            self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class SQLiteSink(DynamicSink):
    def __init__(self, db_path):
        self.db_path = db_path

    def build(self, step_id: str, worker_index: int, worker_count: int) -> SQLiteSinkPartition:
        return SQLiteSinkPartition(self.db_path)


# Build the Bytewax dataflow
flow = Dataflow("device_metrics_batching")

# 1. Read device metrics from JSONL file
stream = op.input("input", flow, JSONLSource(input_path))

# 2. Filter out any metrics where metric_value is less than 0
filtered_stream = op.filter("filter_negatives", stream, lambda x: x.get("metric_value", 0) >= 0)

# 3. Group the metrics by device_id (key_on)
keyed_stream = op.key_on("key_by_device", filtered_stream, lambda x: str(x.get("device_id")))

# 4. Batch the metrics for each device optimally using a max batch size of 10 and a timeout of 1 second
collected_stream = op.collect("collect_batches", keyed_stream, timeout=timedelta(seconds=1), max_size=10)

# 5. Write the batched metrics to SQLite database using custom Bytewax Sink
op.output("output", collected_stream, SQLiteSink(db_path))

if __name__ == "__main__":
    print("Running Bytewax dataflow...")
    run_main(flow)
    print("Bytewax dataflow execution completed.")

    # Write "Pipeline finished." to the log file upon successful completion
    with open(log_path, "a") as log_f:
        log_f.write("Pipeline finished.\n")
    print("Pipeline finished written to log file.")
