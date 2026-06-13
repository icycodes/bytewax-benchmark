# Per-User Click Counter with Bytewax Tumbling Windows

## Background
You are building a real-time analytics pipeline that aggregates web clickstream events into fixed time intervals. Use **Bytewax 0.21.1** (a Python-native stream processing framework on Timely Dataflow) to compute the number of clicks each user generates within each 5-minute tumbling window using event-time semantics.

## Requirements
- Build a Bytewax dataflow defined in `dataflow.py` as a top-level `flow` variable.
- Read newline-delimited JSON click events from `input.jsonl` (one JSON object per line). Each event has the following fields:
  - `user_id` (integer): the user that performed the click.
  - `timestamp` (string): ISO-8601 UTC timestamp (e.g. `2026-01-01T00:02:30+00:00`).
  - `page` (string): the page that was clicked.
- Aggregate events using a **5-minute tumbling window** based on **event time** (the `timestamp` field), aligned to `2026-01-01T00:00:00+00:00 UTC`.
- For each (user, window) pair, count the number of clicks observed.
- Write the aggregated results to `output.jsonl` as newline-delimited JSON, where each line is a single JSON object with the following fields:
  - `user_id` (string): the user id of the aggregated user.
  - `window_start` (string): the UTC start of the tumbling window in ISO-8601 format with explicit timezone (e.g. `2026-01-01T00:00:00+00:00`).
  - `count` (integer): the number of clicks for that user in that window.
- The dataflow must run to completion (EOF) when launched via `python -m bytewax.run dataflow:flow` and exit with status 0.

## Implementation Hints
- Bytewax stateful operators (including windowing) require keyed streams of `(key, value)` 2-tuples where the **key is a Python `str`**. Cast integer user ids to strings before applying windowing operators.
- Use `bytewax.operators.windowing.EventClock` together with `TumblingWindower` to drive event-time aggregation. Pick a reasonable `wait_for_system_duration` (`timedelta(seconds=0)` is fine for a finite source).
- `fold_window` / `count_window` emit `(key, (window_id, accumulator))` on their `.down` stream. The window start time can be derived from `align_to + window_id * window_length`.
- Use `bytewax.connectors.files.FileSink` (or a similar sink) to write the JSONL output. The sink serializes each upstream item as one line, so map your records to JSON strings before output.
- A timestamp string such as `2026-01-01T00:02:30+00:00` can be parsed with `datetime.fromisoformat`.

## Acceptance Criteria
- Project path: /home/user/clickstream
- Command: `python -m bytewax.run dataflow:flow`
- The command must be executed from `/home/user/clickstream`.
- The command reads click events from `/home/user/clickstream/input.jsonl` and writes aggregated results to `/home/user/clickstream/output.jsonl`.
- The command must exit with status code 0.
- `output.jsonl` contains one JSON object per line. The set of objects (order is not significant) must equal the set of (user, 5-minute window) pairs present in the input, with the correct click counts. Each line has the shape:
  ```json
  {"user_id": string, "window_start": string, "count": integer}
  ```
  where `window_start` is an ISO-8601 UTC timestamp aligned to `2026-01-01T00:00:00+00:00` on 5-minute boundaries (e.g. `2026-01-01T00:00:00+00:00`, `2026-01-01T00:05:00+00:00`, ...).
- `dataflow.py` must expose the dataflow object as the module-level name `flow`.

