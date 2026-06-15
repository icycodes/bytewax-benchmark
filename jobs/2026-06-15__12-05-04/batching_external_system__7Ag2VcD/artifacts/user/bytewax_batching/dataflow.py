"""
Bytewax dataflow that reads device metrics from a JSONL file,
filters out negative metric_value entries, groups by device_id,
batches with collect(), and bulk-inserts into SQLite.
"""

import json
import logging
import os
import sqlite3
from datetime import timedelta
from typing import List, Tuple

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RUN_ID = os.environ["ZEALT_RUN_ID"]
INPUT_FILE = "/home/user/bytewax_batching/input.jsonl"
DB_PATH = f"/home/user/bytewax_batching/metrics-{RUN_ID}.db"
LOG_FILE = "/home/user/bytewax_batching/output.log"

COLLECT_MAX_SIZE = 10
COLLECT_TIMEOUT = timedelta(seconds=1)

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> None:
    """Create the device_metrics table if it does not exist."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS device_metrics (
            device_id    TEXT,
            timestamp    TEXT,
            metric_value REAL
        )
        """
    )
    conn.commit()
    conn.close()
    logger.info("Database initialised at %s", db_path)


# ---------------------------------------------------------------------------
# Custom Bytewax Sink
# ---------------------------------------------------------------------------

class SQLitePartition(StatelessSinkPartition):
    """Writes batches of (device_id, [metrics]) tuples to SQLite."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)

    def write_batch(self, items: List[Tuple[str, List[dict]]]) -> None:
        """
        items is a list of (device_id, [metric_dict, ...]) tuples produced
        by the collect() operator.
        """
        rows = []
        for device_id, metrics in items:
            for m in metrics:
                rows.append((m["device_id"], m["timestamp"], m["metric_value"]))

        if rows:
            self._conn.executemany(
                "INSERT INTO device_metrics (device_id, timestamp, metric_value) VALUES (?, ?, ?)",
                rows,
            )
            self._conn.commit()
            logger.info("Bulk-inserted %d row(s) into device_metrics", len(rows))

    def close(self) -> None:
        self._conn.close()


class SQLiteSink(DynamicSink):
    """DynamicSink that opens one SQLitePartition per worker."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def build(self, step_id: str, worker_index: int, worker_count: int) -> SQLitePartition:
        return SQLitePartition(self._db_path)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

def parse_line(line: str) -> dict:
    return json.loads(line)


def is_valid(metric: dict) -> bool:
    return metric["metric_value"] >= 0


def get_device_id(metric: dict) -> str:
    return metric["device_id"]


# Initialise DB before building the dataflow
init_db(DB_PATH)

flow = Dataflow("device_metrics_batching")

# 1. Read lines from the JSONL file
raw = op.input("input", flow, FileSource(INPUT_FILE))

# 2. Parse each line as JSON
parsed = op.map("parse", raw, parse_line)

# 3. Filter out metrics with negative values
valid = op.filter("filter_negative", parsed, is_valid)

# 4. Key by device_id so that collect() groups per device
keyed = op.key_on("key_by_device", valid, get_device_id)

# 5. Batch using collect() – up to 10 items or 1-second timeout
batched = op.collect("collect_batches", keyed, COLLECT_TIMEOUT, max_size=COLLECT_MAX_SIZE)

# 6. Write batches to SQLite via the custom sink
op.output("sqlite_output", batched, SQLiteSink(DB_PATH))

# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from bytewax.run import cli_main

    cli_main(flow)

    # Append success marker to the log file
    with open(LOG_FILE, "a") as fh:
        fh.write("Pipeline finished.\n")
    logger.info("Pipeline finished.")
