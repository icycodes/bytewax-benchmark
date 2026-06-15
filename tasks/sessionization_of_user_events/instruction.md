# Sessionization of User Events with Bytewax

## Background
Sessionization is a common stream processing task where user events are grouped into sessions based on inactivity. Bytewax provides windowing operators to handle this dynamically.

## Requirements
Create a Bytewax dataflow in `run.py` that reads user events from a JSON lines file, groups them into session windows based on event time, and writes the summarized sessions to an output JSON lines file.

- Read events from `user_events.jsonl`.
- Group events by `user_id`.
- Use a session window with a 30-minute inactivity gap.
- Use event time for the clock.
- For each session, calculate the total number of events, the start time (timestamp of the first event in the session), and the end time (timestamp of the last event in the session).
- Write the results to `sessions.jsonl`.

## Implementation Hints
- Parse the input JSON lines to extract the `user_id` as the key and the event payload as the value.
- Use Bytewax's `EventClock` and `SessionWindower`.
- Use a windowing operator like `collect_window` to gather events in each session, then map the result to calculate the required metrics.
- Ensure timestamps are parsed into `datetime` objects for the clock and formatted back to ISO 8601 strings for the output.

## Acceptance Criteria
- Project path: /home/user/bytewax-sessionization
- Command: `python run.py`
- The script must read from `user_events.jsonl` and write to `sessions.jsonl` in the same directory.
- `user_events.jsonl` contains JSON objects with `user_id` (string), `event_type` (string), and `timestamp` (ISO 8601 string).
- `sessions.jsonl` must contain exactly one JSON object per session with the following format:
  ```json
  {
    "user_id": "<user_id>",
    "session_start": "<iso_8601_string>",
    "session_end": "<iso_8601_string>",
    "event_count": <integer>
  }
  ```
- The output must correctly reflect 30-minute session gaps based on the event time.

