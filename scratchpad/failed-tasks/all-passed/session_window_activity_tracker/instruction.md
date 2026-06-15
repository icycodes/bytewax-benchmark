# Bytewax Session Windowing for User Activity

## Background
Bytewax is a stateful stream processing framework. In many analytics use cases, events from a single user need to be grouped into "sessions" based on periods of activity and inactivity. You will build a Bytewax dataflow that reads user events, groups them into sessions using a session window, and outputs the collected events per session.

## Requirements
- Build a Bytewax dataflow in `flow.py` that reads JSON lines from `input.jsonl`.
- Parse each line into a dictionary. Each event contains `user_id`, `timestamp` (ISO8601 format), and `event_type`.
- Group the events by `user_id`.
- Use a `SessionWindower` with a gap of 5 seconds to group events into sessions.
- Use an `EventClock` based on the event's `timestamp`. Since this is a bounded file source, set `wait_for_system_duration` to 0.
- Collect the `event_type` for all events in each session.
- Write the output to `output.jsonl` as JSON lines.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` or `CSVSource`/`DirSource` equivalent for reading, or simply a custom input connector if preferred. `bytewax.connectors.files.FileSource` or `DirSource` reading line by line is typical.
- Use `bytewax.connectors.files.FileSink` or `DirSink` for writing the output.
- Map the input to a tuple of `(user_id, event_dict)` so it can be keyed.
- Use `bytewax.operators.windowing.collect_window` with your `EventClock` and `SessionWindower`.
- After windowing, map the `(key, (window_id, items))` tuple into the required output dictionary format before converting to a JSON string and writing it to the sink.

## Acceptance Criteria
- Project path: `/home/user/project`
- Command: `python -m bytewax.run flow`
- Input file format (`/home/user/project/input.jsonl`):
  Each line is a JSON object with `user_id` (string), `timestamp` (ISO8601 string), and `event_type` (string).
- Output file format (`/home/user/project/output.jsonl`):
  Each line must be a valid JSON object containing exactly two fields:
  - `user_id`: The user ID (string).
  - `events`: An array of `event_type` strings collected in that session, preserving their chronological order.
- The window gap must be exactly 5 seconds.

