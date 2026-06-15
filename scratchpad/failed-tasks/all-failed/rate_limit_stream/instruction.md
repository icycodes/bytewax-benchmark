# Stateful Rate Limiting in Bytewax

## Background
Bytewax allows building stateful stream processing pipelines. In this task, you will implement a Token Bucket rate limiting algorithm to throttle an input stream of events per user.

## Requirements
- Build a Bytewax dataflow that reads an input stream of JSON events, applies a token-bucket rate limit per user, and writes the allowed events to an output JSON file.
- The rate limiter must maintain its state using `op.stateful_map`.
- The pipeline state must be fully picklable to support Bytewax's SQLite recovery mechanism.

## Implementation Hints
- Use `op.stateful_map` to maintain the token bucket state per `user_id`.
- The state should be a simple serializable object (e.g., a tuple of `(current_tokens, last_update_timestamp)`) to ensure picklability.
- Remember to cast the `user_id` to a string before using it as a key for stateful operations, as Bytewax requires string keys.
- You can use the `FileSource` and `FileSink` from `bytewax.connectors.files` (or similar standard connectors) to read and write JSON lines.
- To drop throttled events, your `stateful_map` can emit `None` for them, and a subsequent `op.filter` can remove the `None` values.

## Acceptance Criteria
- Project path: /home/user/bytewax_project
- Command: `python -m bytewax.run dataflow:flow`
- Input: The pipeline must read from `/home/user/bytewax_project/input.jsonl`. Each line is a JSON object with `user_id` (string), `event_id` (string), and `timestamp` (float representing epoch seconds).
- Output: The pipeline must write only the allowed events to `/home/user/bytewax_project/output.jsonl` as JSON objects, one per line.
- Rate Limiting Logic:
  - Token bucket per `user_id`.
  - Max capacity = 5.0 tokens.
  - Initial tokens = 5.0.
  - For each incoming event, calculate the elapsed time since the `timestamp` of the previously seen event for that user (or 0 if it's the first event).
  - Add `elapsed_time * 1.0` to the current token count, capping at the maximum capacity of 5.0.
  - If the token count is >= 1.0, subtract 1.0 and allow the event.
  - If the token count is < 1.0, do not subtract and drop the event.
  - The state's last timestamp must be updated to the current event's `timestamp` in both cases (whether allowed or dropped).
- Fault Tolerance: The state used in `stateful_map` must be picklable. The verifier will run the pipeline with recovery enabled.
