# Complex JSON Stream Filtering with Bytewax

## Background
You are building a streaming application using Bytewax to process IoT telemetry data. The data stream contains mixed JSON events for device configuration and metric reports. The pipeline must dynamically update alerting thresholds and filter metrics based on the latest state.

## Requirements
- Create a Bytewax dataflow in `pipeline.py` that reads from `input.jsonl`.
- Parse each line as JSON. Gracefully ignore any lines that are invalid JSON or missing required fields (`type`, `device_id`, `payload`).
- Events with `"type": "config"` contain a `"threshold"` inside `payload`. Use this to update the temperature threshold for the specific `device_id`.
- The default temperature threshold for any device is `100.0`.
- Events with `"type": "metric"` contain a `"temperature"` inside `payload`. If the temperature is strictly greater than the device's current threshold, emit an alert.
- The alert must be a JSON string: `{"device_id": "...", "alert_type": "temperature_high", "value": <temp>, "threshold": <current_threshold>}`.
- Read the `run-id` from the `ZEALT_RUN_ID` environment variable.
- Write the resulting alert JSON strings to `output-${run-id}.jsonl`.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` and `FileSink` for I/O.
- Use `op.filter_map` or a combination of `op.map` and `op.filter` to parse JSON and drop invalid/malformed records.
- Ensure the stream is keyed by `device_id` (must be a string) before applying stateful operations.
- Use `op.stateful_map` to maintain the threshold state per device and process the events.
- Format the output as JSON strings before sending to `FileSink`.

## Acceptance Criteria
- Project path: /home/user/bytewax-task
- Command: `python -m bytewax.run pipeline:flow`
- The command must successfully execute and produce `output-${run-id}.jsonl` (where `run-id` is read from the `ZEALT_RUN_ID` environment variable).
- The output file must contain exactly the expected alert JSON objects, one per line, based on the dynamic thresholds.
- The pipeline must not crash when encountering invalid JSON or missing fields.
