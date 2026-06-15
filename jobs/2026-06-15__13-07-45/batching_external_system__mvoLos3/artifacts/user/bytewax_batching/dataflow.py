import json
import os
import sqlite3
import logging
from datetime import timedelta
from pathlib import Path
from typing import List

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RUN_ID = os.environ["ZEALT_RUN_ID"]
INPUT_PATH = Path("/home/user/bytewax_batching/input.jsonl")
DB_PATH = Path(f"/home/user/bytewax_batching/metrics-{RUN_ID}.db")
LOG_PATH = Path("/home/user/bytewax_batching/output.log")

BATCH_MAX_SIZE = 10
BATCH_TIMEOUT = timedelta(seconds=1)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH), mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLite Sink
# ---------------------------------------------------------------------------
class SQLiteSinkPartition(StatelessSinkPartition):
    """A stateless sink partition that batch-inserts rows into SQLite."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS device_metrics ("
            "  device_id TEXT,"
            "  timestamp TEXT,"
            "  metric_value REAL"
            ")"
        )
        self._conn.commit()
        logger.info("SQLite table 'device_metrics' ready at %s", db_path)

    def write_batch(self, items: List[tuple]) -> None:
        """Write a batch of (device_id, records) tuples to SQLite."""
        rows = []
        for device_id, records in items:
            for rec in records:
                rows.append((device_id, rec["timestamp"], rec["metric_value"]))

        if rows:
            self._conn.executemany(
                "INSERT INTO device_metrics (device_id, timestamp, metric_value) "
                "VALUES (?, ?, ?)",
                rows,
            )
            self._conn.commit()
            logger.info("Inserted %d rows into device_metrics", len(rows))

    def close(self) -> None:
        self._conn.close()
        logger.info("SQLite connection closed.")


class SQLiteSink(DynamicSink):
    """Dynamic sink that creates a SQLite partition per worker."""

    def __init__(self, db_path: str):
        self._db_path = db_path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return SQLiteSinkPartition(self._db_path)


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------
def build_dataflow() -> Dataflow:
    flow = Dataflow("device_metrics_batching")

    # 1. Read lines from the JSONL file
    lines = op.input("read_jsonl", flow, FileSource(str(INPUT_PATH)))

    # 2. Parse JSON and filter out negative metric_value
    def parse_and_filter(line: str):
        record = json.loads(line)
        if record["metric_value"] >= 0:
            return record
        return None

    parsed = op.filter_map("parse_and_filter", lines, parse_and_filter)

    # 3. Key by device_id so collect groups per device
    keyed = op.key_on("key_by_device", parsed, lambda r: r["device_id"])

    # 4. Collect into batches: max 10 records or 1 second timeout
    batched = op.collect("batch", keyed, timeout=BATCH_TIMEOUT, max_size=BATCH_MAX_SIZE)

    # 5. Write batches to SQLite
    op.output("write_to_sqlite", batched, SQLiteSink(str(DB_PATH)))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting dataflow for run-id=%s", RUN_ID)
    logger.info("Input: %s", INPUT_PATH)
    logger.info("Output DB: %s", DB_PATH)

    flow = build_dataflow()
    run_main(flow)

    logger.info("Pipeline finished.")
