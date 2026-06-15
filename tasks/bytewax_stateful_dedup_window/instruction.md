# Bytewax Stateful Deduplication

## Background
Bytewax is a Python stateful stream processing framework. In many streaming applications, you receive duplicate events that need to be filtered out. In this task, you will use Bytewax's stateful processing capabilities to deduplicate events over a sliding time window.

## Requirements
- Create a Bytewax dataflow in `run.py` that reads JSON objects from `input.jsonl`.
- Each JSON object has the following format: `{"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:00Z"}`.
- The dataflow must group events by `user_id`.
- Implement a stateful deduplication logic using Bytewax's `stateful` operator and `StatefulLogic`.
- An event should be emitted to the downstream only if its `event_id` has not been seen for the same `user_id` within the last 10 seconds (based on the event's `timestamp`).
- If the same `event_id` arrives within 10 seconds of the previous occurrence (inclusive of exactly 10 seconds, meaning `event_timestamp <= previous_timestamp + 10s`), it should be dropped. If it arrives strictly after 10 seconds, it should be emitted and the 10-second window restarts for that `event_id`.
- The state must be cleaned up dynamically: any `event_id` that is older than 10 seconds from the *current event's timestamp* should be removed from the state to prevent memory leaks.
- The deduplicated events must be written as JSON strings to `output.jsonl`.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` and `FileSink` (or `DirSink` depending on Bytewax 0.21 API) to read and write files, or write a custom input/output logic if needed.
- Parse the JSON strings into Python dictionaries and parse the ISO 8601 timestamps into `datetime` objects.
- To use the `stateful` operator, map your stream into a keyed stream: `(user_id, event_dict)`.
- Create a class inheriting from `bytewax.operators.StatefulLogic`. In the `on_item` method, implement the deduplication and state cleanup logic.
- The state should map `event_id` to the timestamp of its last emitted occurrence.
- Format the output back to JSON strings before writing to the sink.
- Execute the dataflow using `bytewax.testing.run_main` or the `bytewax.run` CLI.

## Acceptance Criteria
- Project path: /home/user/myproject
- Command: python3 run.py
- Input file: `/home/user/myproject/input.jsonl` (provided before running the command)
- Output file: `/home/user/myproject/output.jsonl`
- The output file must contain only the deduplicated events, in the same relative order as the input, formatted as valid JSON strings (one per line).

