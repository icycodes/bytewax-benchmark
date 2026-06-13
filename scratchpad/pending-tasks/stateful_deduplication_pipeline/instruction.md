# Bytewax Stateful Deduplication Pipeline

## Background
You are building a real-time event processing pipeline using **Bytewax 0.21.1**, a Python-native stateful stream processing framework. The pipeline must deduplicate user events received from an upstream system. Each event includes a `message_id`, a `user_id`, and a `payload`. Due to upstream retries and at-least-once delivery semantics, many duplicate events with the same `message_id` arrive. The downstream consumer requires that only the FIRST occurrence of each `message_id` per user be retained.

Your job is to implement a Bytewax dataflow that uses a stateful operator to remember seen `message_id`s per user and to filter out duplicates. The pipeline must read from a JSON-lines input file and write the unique events to a JSON-lines output file.

## Requirements
- Implement a Bytewax dataflow named `dedup_flow` in the file `/home/user/myproject/pipeline.py`.
- Read events one per line from `/home/user/myproject/data/events.jsonl`. Each line is a JSON object with the fields `message_id` (string), `user_id` (any JSON-serializable value), and `payload` (any JSON-serializable value).
- Partition state by `user_id`, remembering the set of `message_id`s that have already been observed for that user.
- Emit only the first occurrence of every `(user_id, message_id)` pair. Duplicate events sharing the same `(user_id, message_id)` must be dropped silently.
- Write each emitted (unique) event as a single JSON object per line to `/home/user/myproject/data/unique_events.jsonl`. The output must contain the original `message_id`, `user_id`, and `payload` fields.
- The pipeline must be runnable with `python -m bytewax.run pipeline:dedup_flow` from the project directory and must exit with code 0.

## Implementation Hints
- Use the `bytewax.connectors.files.FileSource` connector to read lines from the input file and `bytewax.connectors.files.FileSink` to write lines to the output file.
- Use `op.key_on` to create a Keyed Stream partitioned by `user_id`. Bytewax v0.21.1 requires keys to be Python strings — cast non-string user IDs accordingly.
- Use `op.stateful_map` (from `bytewax.operators`) to remember which `message_id`s have already been seen per user, and use `op.filter` to drop duplicates.
- Make sure the state objects returned by the stateful logic are picklable and not mutated in place across emissions, so the pipeline remains compatible with Bytewax's recovery snapshotting.
- Use Python's `json` module to parse incoming lines and to serialize outgoing events.

## Acceptance Criteria
- Project path: /home/user/myproject
- Command: python -m bytewax.run pipeline:dedup_flow
- Working directory for the command: /home/user/myproject
- The command must exit with status code 0.
- The output file `/home/user/myproject/data/unique_events.jsonl` must exist after the command finishes.
- Each line of the output file must be a valid JSON object containing the keys `message_id`, `user_id`, and `payload`.
- For every `(user_id, message_id)` pair that appears in the input file, exactly one corresponding output line must be produced, and that line must preserve the `payload` of the FIRST input occurrence of that pair.
- No duplicate `(user_id, message_id)` pair may appear more than once in the output file.
- The output must contain only `(user_id, message_id)` pairs that exist in the input file.

