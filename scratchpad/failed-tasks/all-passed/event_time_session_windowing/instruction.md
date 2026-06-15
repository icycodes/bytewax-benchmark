# Session-Based Clickstream Aggregator

## Background
You need to implement a session-based clickstream aggregator using Bytewax. The pipeline will group user click events into dynamic session windows based on inactivity gaps, enabling real-time computation of metrics like total pages visited per session.

## Requirements
- Implement a Bytewax dataflow in `pipeline.py` named `flow`.
- Read timestamped user click events from `input.jsonl`. Each line is a JSON object with `user_id`, `page`, and `timestamp` (ISO 8601 string, UTC).
- Group the events into dynamic session windows using event-time. A session is closed after 10 seconds of inactivity for a given user.
- For each session, calculate the total number of pages visited.
- Write the output to `output.jsonl` where each line is a JSON object containing `user_id` and `total_pages`.

## Implementation Hints
- Use `bytewax.operators.windowing` with `EventClock` and `SessionWindower`.
- You will need to parse the ISO 8601 string into a timezone-aware `datetime` object for the `EventClock`.
- Remember that Bytewax stateful operators require the stream to be keyed (a tuple of `(key, value)`) and the key must be a string.
- Use `bytewax.connectors.files.FileSource` and a custom sink or standard file sink to handle JSONL I/O.
- The output of `fold_window` includes window metadata. You will need to map this to the expected output format before writing.

## Acceptance Criteria
- Project path: /home/user/project
- Command: `python -m bytewax.run pipeline:flow`
- The input file `input.jsonl` will contain JSON objects, one per line:
  ```json
  {"user_id": "string", "page": "string", "timestamp": "string"}
  ```
- The command must read `input.jsonl` and successfully process the dataflow.
- The output file `output.jsonl` must be created and contain JSON objects, one per line:
  ```json
  {"user_id": "string", "total_pages": "number"}
  ```
- The output must correctly reflect the total pages per session for each user, grouped by 10-second inactivity windows based on the event timestamps.

