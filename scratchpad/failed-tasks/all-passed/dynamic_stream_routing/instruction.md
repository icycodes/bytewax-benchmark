# Bytewax Dynamic Stream Routing

## Background
Build a Bytewax stream processing dataflow that reads JSON events and dynamically routes them to multiple different output sinks based on the payload content.

## Requirements
- Implement a Python script that runs a Bytewax dataflow.
- The dataflow should read a JSONL file containing events.
- Each event is a JSON object. The dataflow must route the events into four different categories based on the `type` field:
  - `error`: Events with `"type": "error"`
  - `metric`: Events with `"type": "metric"`
  - `log`: Events with `"type": "log"`
  - Dead letter: Events missing the `type` field or having an unknown `type`.
- The dataflow must write the categorized events into four distinct output JSONL files.
- You must use Bytewax operators (e.g., `branch` or `filter`) to achieve this routing.

## Implementation Hints
- Use Bytewax's `FileSource` or a custom input connector to read the JSONL file.
- In Bytewax v0.18+, you can branch streams or apply multiple `filter` operators to the same upstream to route data to multiple downstream branches.
- Use `FileSink` or standard Python file I/O within a custom sink to write the output files.
- Parse the JSON strings into Python dictionaries to inspect the `type` field, then route them accordingly, and serialize them back to JSON strings before writing to the sinks.

## Acceptance Criteria
- Project path: /home/user/bytewax_routing
- Command: `python run.py --input <input_file>`
- The command must read the specified `<input_file>`.
- Expected output files in the project directory after execution:
  - `errors.jsonl`
  - `metrics.jsonl`
  - `logs.jsonl`
  - `dead_letter.jsonl`
- Each output file must contain only the events corresponding to its category, preserving the original JSON object structure, one per line.

