# Merge Multiple Streams into a Unified Profile

## Background
You need to build a stateful stream processing pipeline using Bytewax (v0.21.1) to merge user data from three different sources into a single, unified user profile. The data sources are already provided in the project directory.

## Requirements
- Create a Bytewax dataflow in `merge_flow.py`.
- Read data from three pre-existing local JSONL files: `users.jsonl`, `emails.jsonl`, and `activities.jsonl`.
- The schema for each file differs:
  - `users.jsonl`: `{"user_id": <int or str>, "name": <str>}`
  - `emails.jsonl`: `{"id": <int or str>, "email_address": <str>}`
  - `activities.jsonl`: `{"uid": <int or str>, "last_login": <str>}`
- Join the three streams on the user ID to create a unified profile.
- The output must be a single stream of JSON strings, where each JSON object has the exact keys: `id` (string), `name` (string), `email` (string), and `last_login` (string).
- The join must be an inner join (only output profiles that have data from all three sources).
- Output the final JSON strings to standard output.

## Acceptance Criteria
- Project path: `/home/user/bytewax_project`
- Command: `python -m bytewax.run merge_flow:flow`
- The command input format: The dataflow reads `users.jsonl`, `emails.jsonl`, and `activities.jsonl` from the current directory.
- The expected command output format: The stdout should print JSON strings, one per line. Each JSON string must contain the exact keys `id`, `name`, `email`, and `last_login`. There should be no other output except the JSON lines.

