# Bytewax Stateful Event Deduplication

## Background
You are building a real-time event processing pipeline using Bytewax v0.21.1. Clients sometimes send duplicate events due to network retries. You need to deduplicate these events using a time-based window approach with the `stateful_map` operator.

## Requirements
- Implement a Bytewax dataflow in `run_pipeline.py`.
- Read input events from `events.json` (JSON lines format). Each event has a `user_id`, `event_id`, `timestamp` (ISO 8601 string), and `payload`.
- Key the stream by `user_id`.
- Use `op.stateful_map` to maintain a state of seen `event_id`s and their timestamps for each user.
- **Deduplication Logic**: If an event arrives and its `event_id` was already seen within the last 10 seconds (based on the event's timestamp, not system time), it is a duplicate and should not be emitted downstream.
- **State Pruning**: To prevent memory leaks, your stateful logic MUST prune old events from the state (i.e., remove any `event_id`s that are older than 10 seconds compared to the current event's timestamp).
- Write the downstream (deduplicated) events to `output.json` (JSON lines format).

## Implementation Hints
- Read `events.json` and parse the JSON. Yield them into the dataflow.
- Use `op.key_on` to create a Keyed Stream keyed by `user_id`. (Remember that Bytewax requires keys to be strings).
- In your `stateful_map` mapper function, parse the ISO 8601 `timestamp`. Check if the `event_id` exists in the state and if its timestamp is within 10 seconds of the current event. 
- If it's a duplicate, you can emit `None` and filter it out later using `op.filter` or `op.filter_map`.
- Clean up the state by removing entries older than 10 seconds relative to the current event's timestamp.
- Format the output back to JSON and write to `output.json`.

## Acceptance Criteria
- Project path: /home/user/bytewax_project
- Command: `python run_pipeline.py`
- Input: `events.json` containing JSON lines.
- Output: `output.json` containing JSON lines of the deduplicated events. The output order must match the input order of the non-duplicate events.

