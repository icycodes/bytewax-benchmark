# Session-Based Clickstream Aggregator with Bytewax

## Background
You are building a clickstream analytics pipeline with **Bytewax (v0.21.1)** — a Python-native stateful stream processing framework. The product team needs to compute per-user browsing sessions from a stream of click events so that downstream services (recommendation, churn detection) can reason about user intent within bounded sessions.

A session is defined as a contiguous run of click events for the same user where the gap between consecutive events does **not** exceed 10 seconds. Whenever a user is idle for more than 10 seconds, the next click starts a new session.

## Requirements
- Build a Bytewax dataflow named `flow` (as a module-level attribute) in `/home/user/myproject/pipeline.py` that can be executed via `python -m bytewax.run pipeline:flow`.
- The pipeline must read newline-delimited JSON click events from `/home/user/myproject/input.jsonl`. Each line has the shape:
  - `user_id`: either a string or an integer.
  - `page`: a string (the visited URL path).
  - `timestamp`: an ISO 8601 UTC string (e.g. `"2026-01-01T12:00:05+00:00"` or `"2026-01-01T12:00:05Z"`).
- Group events into sessions per `user_id` using event-time semantics. A session closes after 10 seconds of inactivity.
- For every closed session emit exactly one JSON object summarizing the session.
- Write each emitted summary as a single JSON line to `/home/user/myproject/output.jsonl`.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` and `bytewax.connectors.files.FileSink` so the pipeline reads and writes real files instead of using in-memory testing sources.
- The `FileSink` requires the destination file to already exist on disk — create or truncate it inside `pipeline.py` before defining the sink.
- Bytewax stateful operators (windowing, joins, `stateful_map`) require keyed streams of `(key, value)` tuples where the key is a `str`. Cast every `user_id` to a string before keying.
- Use `bytewax.operators.windowing.EventClock` to derive event time from each record's `timestamp` field, and `bytewax.operators.windowing.SessionWindower` with a 10-second gap to materialize sessions.
- A fold-style window aggregator (`fold_window` or `collect_window`) is convenient for collecting the ordered list of pages per session.
- The final sink writes strings, so map each window result to a JSON string before calling `op.output`.

## Acceptance Criteria
- Project path: /home/user/myproject
- Pipeline module: /home/user/myproject/pipeline.py exposing a top-level Bytewax `Dataflow` named `flow`.
- Input file: /home/user/myproject/input.jsonl (newline-delimited JSON, one click event per line).
- Output file: /home/user/myproject/output.jsonl (newline-delimited JSON, one session summary per line).
- Command: `python -m bytewax.run pipeline:flow` (must exit with status code 0).
- Each output line must be a JSON object containing at least these fields:
  - `user_id`: string (the user identifier; integer ids in the input must be stringified).
  - `page_count`: integer, the number of click events belonging to the session.
  - `pages`: array of strings, the `page` values of the session in event-time order.
- Two events for the same user separated by **more than 10 seconds** must belong to **different** sessions; two events separated by **10 seconds or less** must belong to the **same** session.
- Each distinct `(user_id, session)` pair must appear in exactly one output line. Running the command on the same input must yield equivalent sessions regardless of input line ordering.
- The pipeline must not raise an exception when an input line has an integer `user_id` (Bytewax requires string keys for stateful operators).

