# Stateful Join of Event Streams with Bytewax

## Background
In ad-tech and analytics, joining separate event streams is a common requirement. You need to build a stateful stream processing pipeline using Bytewax to join an impressions stream and a clicks stream based on a shared user ID.

## Requirements
- Create a Bytewax dataflow in `flow.py`.
- Read two input streams from local JSONL files: `impressions.jsonl` and `clicks.jsonl`.
- Each line in the files is a JSON object containing a `user_id` field and other event-specific data.
- Perform a stateful join on the two streams using the `user_id` as the key.
- The join must be a complete join (inner join), emitting a combined record only when both an impression and a click have been seen for a given `user_id`.
- Write the joined results as JSON strings to `joined.jsonl`.

## Implementation Hints
- Use Bytewax's `operators` to construct the dataflow.
- You will need to parse the JSON lines and extract the `user_id` to use as the state key.
- Use `op.key_on` to key the streams and `op.join` to perform the stateful join.
- You can use standard Python file I/O or Bytewax file connectors for reading and writing.
- Bytewax expects a specific format for the output of a join, typically a tuple of `(key, (value1, value2))`. Format this into a JSON string before writing to the sink.

## Acceptance Criteria
- Project path: /home/user/project
- Command: python -m bytewax.run flow:flow
- The input files `impressions.jsonl` and `clicks.jsonl` will be present in the project directory.
- The pipeline must read these files and perform a complete join on `user_id`.
- The pipeline must output to `joined.jsonl` in the project directory.
- Each line in `joined.jsonl` must be a valid JSON object containing the `user_id` and the joined data from both streams.

