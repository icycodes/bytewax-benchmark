# Bytewax Batching and Micro-Batching Pipeline

## Background
Build a Bytewax pipeline that implements both custom source batching and downstream micro-batching. You need to read events from an SQLite database in batches, group them by user, and micro-batch them before writing to standard output.

## Requirements
- Create a Bytewax pipeline in `/home/user/app/pipeline.py` with a `Dataflow` named `batching_flow` assigned to the variable `flow`.
- Implement a custom `FixedPartitionedSource` and `StatefulSourcePartition` to read from an SQLite database at `/home/user/app/events.db`.
- The table `events` has columns: `id` (INTEGER PRIMARY KEY), `user_id` (TEXT), `event_type` (TEXT), `payload` (TEXT).
- The source must have exactly 1 partition (e.g., named `"single"`).
- `next_batch()` must fetch up to 5 records per call, ordered by `id` ASC. It should return a list of dictionaries with keys `id`, `user_id`, `event_type`, `payload`. If there are no more records, it should raise `StopIteration`.
- `snapshot()` must return the last processed `id` to support exactly-once processing (or `None`/`0` if none processed).
- Key the stream by `user_id`.
- Use the `op.collect` operator to micro-batch events for each user. Set the timeout to 1 second and `max_size` to 3.
- Map the output of `collect` to a JSON string with the format: `{"user_id": "<user_id>", "events": [<list_of_event_dicts>]}`.
- Write the JSON strings to standard output using `StdOutSink`.

## Implementation Hints
- Use `sqlite3` to connect to the database. Ensure the connection is established in the partition, not the source, to avoid pickling errors during recovery.
- The `resume_state` passed to `build_part` will be the last `id` yielded. Use this to construct your `SELECT` query with `WHERE id > ?`.
- When fetching records, if no records are returned, raise `StopIteration` to signal the end of the partition.
- `op.collect` returns a stream of `(key, list_of_items)`. Use `op.map` to convert this tuple to the required JSON string format before passing it to `StdOutSink`.
- `StdOutSink` requires the items in the stream to be strings.

## Acceptance Criteria
- Project path: /home/user/app
- Command: `python -m bytewax.run pipeline:flow`
- The script must successfully process all records from the database and exit.
- The standard output must contain the JSON serialized micro-batches, one per line.
- The micro-batching must respect the `max_size=3` limit, meaning a user with 5 events will be split into a batch of 3 and a batch of 2.
