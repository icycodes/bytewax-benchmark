# Bytewax Windowed Word Count

## Background
Implement a stream processing dataflow using Bytewax that counts the occurrences of words within specific time windows based on event time.

## Requirements
- Read a JSON Lines (JSONL) input file containing events with a word and a timestamp.
- Use event time processing.
- Group the words using a 1-hour tumbling window aligned to `2023-01-01T00:00:00Z`.
- Count the occurrences of each word within each window.
- Write the results to a JSON Lines (JSONL) output file.

## Implementation Hints
- Use `bytewax.operators.windowing.count_window`.
- Use `EventClockConfig` to extract the timestamp from each event. You will need to parse the ISO8601 string into a `datetime` object with a UTC timezone.
- Use `TumblingWindow` for the 1-hour windows.
- Bytewax window operators expect keyed data `(key, value)`. You should key by the word.
- The downstream output of `count_window` yields tuples like `(key, (window_id, count))`. Format this into a dictionary before writing to the output file.

## Acceptance Criteria
- Project path: `/home/user/myproject`
- Command: `python dataflow.py <input_file> <output_file>`
- Input format: JSON Lines where each line is `{"time": "ISO8601_string", "word": "string"}`.
- Output format: JSON Lines where each line is `{"word": "string", "window_id": "ISO8601_string", "count": integer}`. The `window_id` should be the string representation of the window's start time in UTC.

