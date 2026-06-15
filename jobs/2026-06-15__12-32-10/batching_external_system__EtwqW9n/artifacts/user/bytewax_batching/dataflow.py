#!/usr/bin/env python3
"""Bytewax dataflow that reads device metrics from JSONL, filters negative values,
groups by device_id, batches, and writes to SQLite via bulk inserts."""

import json
import os
import sqlite3
from datetime import timedelta
from typing import List

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main
import bytewax.operators as op


# ── Configuration ──────────────────────────────────────────────────────────
RUN_ID = os.environ["ZEALT_RUN_ID"]
INPUT_PATH = "/home/user/bytewax_batching/input.jsonl"
DB_PATH = f"/home/user/bytewax_batching/metrics-{RUN_ID}.db"
LOG_PATH = "/home/user/bytewax_batching/output.log"


# ── SQLite Sink ────────────────────────────────────────────────────────────
class SQLiteSinkPartition(StatelessSinkPartition):
    """Receives batches of metrics and performs bulk inserts into SQLite."""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def write_batch(self, items: List) -> None:
        """items is a list of (device_id, [metric_dicts]) tuples."""
        for device_id, metrics in items:
            rows = [
                (m["device_id"], m["timestamp"], m["metric_value"])
                for m in metrics
            ]
            self.cursor.executemany(
                "INSERT INTO device_metrics (device_id, timestamp, metric_value) "
                "VALUES (?, ?, ?)",
                rows,
            )
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class SQLiteSink(DynamicSink):
    """Custom Bytewax sink that writes batched metrics to SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return SQLiteSinkPartition(self.db_path)


# ── Helper functions ───────────────────────────────────────────────────────
def parse_json(line: str):
    """Parse a JSON line into a Python dict."""
    return json.loads(line)


def filter_negative(metric):
    """Keep only metrics with metric_value >= 0."""
    return metric["metric_value"] >= 0


# ── Create DB table before dataflow starts ─────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS device_metrics (
        device_id  TEXT,
        timestamp  TEXT,
        metric_value REAL
    )
    """
)
conn.commit()
conn.close()


# ── Build the dataflow ────────────────────────────────────────────────────
flow = Dataflow("metrics_batching")

# 1. Read JSONL lines from file
lines = op.input("read_jsonl", flow, FileSource(INPUT_PATH))

# 2. Parse each line as JSON
metrics = op.map("parse_json", lines, parse_json)

# 3. Filter out negative metric_value
positive_metrics = op.filter("filter_negative", metrics, filter_negative)

# 4. Key by device_id for grouping
keyed = op.key_on("key_on_device", positive_metrics, lambda m: m["device_id"])

# 5. Collect into batches: max_size=10, timeout=1 second
batched = op.collect("batch", keyed, timedelta(seconds=1), max_size=10)

# 6. Write batched metrics to SQLite
op.output("write_sqlite", batched, SQLiteSink(DB_PATH))


# ── Execute ────────────────────────────────────────────────────────────────
run_main(flow)

# Log successful completion
with open(LOG_PATH, "a") as f:
    f.write("Pipeline finished.\n")