# Bytewax Batching to SQLite

## Background
You have a high-throughput stream of device metrics that needs to be written to a SQLite database. To optimize database performance, you must collect and batch these metrics before performing bulk inserts, rather than inserting them one by one.

## Requirements
- Build a Bytewax dataflow that reads device metrics from a JSONL file.
- Filter out any metrics where `metric_value` is less than 0.
- Group the metrics by `device_id`.
- Batch the metrics for each device optimally using a maximum batch size and a timeout.
- Write the batched metrics to a SQLite database using a custom Bytewax Sink that performs bulk inserts.

## Implementation Hints
- Read the current `run-id` from the `ZEALT_RUN_ID` environment variable.
- Use Bytewax's `collect` operator to handle the batching logic with both size and time constraints.
- Implement a custom Bytewax Sink (e.g., using `DynamicSink` and `StatelessSinkPartition`) to receive the batches and execute bulk inserts (`executemany`) into SQLite.
- Ensure the SQLite table is created before the dataflow starts writing.
- Name the SQLite database file `metrics-${run-id}.db`.

## Acceptance Criteria
- Project path: /home/user/bytewax_batching
- Ensure the script is executed and the SQLite database is populated.
- The Bytewax dataflow script must be executed via `python3 dataflow.py`.
- Input file: `/home/user/bytewax_batching/input.jsonl` (provided in the environment).
- SQLite database path: `/home/user/bytewax_batching/metrics-${run-id}.db` where `run-id` is read from `ZEALT_RUN_ID`.
- The database must contain a table `device_metrics` with columns `device_id` (TEXT), `timestamp` (TEXT), and `metric_value` (REAL).
- The dataflow must use the `collect` operator with `max_size=10` and `timeout` of 1 second.
- The dataflow must write to the database using bulk inserts (batches of records).
- Log file: `/home/user/bytewax_batching/output.log` must contain a line `Pipeline finished.` upon successful completion.

