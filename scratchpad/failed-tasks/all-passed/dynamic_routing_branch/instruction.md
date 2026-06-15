# Dynamic Routing of Sensor Data

## Background
You are building a real-time IoT sensor data processing pipeline using Bytewax. The pipeline needs to read JSON records from a file, route them to different sinks based on their content, transform specific fields, and capture malformed records into an error sink.

## Requirements
- Create a Bytewax dataflow that reads lines from `sensors.json`.
- Parse each line as JSON.
- If a line is not valid JSON, or is missing the `sensor_type` field, route it to an error stream. The error stream should output JSON strings in the format: `{"error": "invalid json", "raw": "<original_string>"}` or `{"error": "missing sensor_type", "raw": "<original_string>"}`.
- If `sensor_type` is `"temperature"`, route it to a temperature stream. Convert the `value_c` field from Celsius to Fahrenheit (`value_f = value_c * 9/5 + 32`), remove the `value_c` field, and add the `value_f` field.
- If `sensor_type` is `"humidity"`, route it to a humidity stream without modification.
- Write the error stream to `errors.json`.
- Write the temperature stream to `temperature.json`.
- Write the humidity stream to `humidity.json`.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` and `FileSink`.
- Use `bytewax.operators.branch` to dynamically route records.
- You may need multiple branches (e.g., one for valid vs invalid, one for temperature vs humidity).
- Ensure the items are converted back to JSON strings before sending them to `FileSink`.

## Acceptance Criteria
- Project path: `/home/user/bytewax_routing`
- Command: `python dataflow.py`
- The script must read from `/home/user/bytewax_routing/sensors.json`.
- The script must output to three files in `/home/user/bytewax_routing`: `temperature.json`, `humidity.json`, and `errors.json`.
- The output files must contain exactly the expected JSON strings, one per line.

