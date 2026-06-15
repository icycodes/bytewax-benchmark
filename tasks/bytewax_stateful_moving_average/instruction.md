# Bytewax Stateful Moving Average

## Background
Build a stateful stream processing pipeline using Bytewax that computes the moving average of sensor temperature readings.

## Requirements
- Create a Bytewax dataflow named `flow` in `pipeline.py`.
- The dataflow should read sensor readings from `input.csv` in the current directory.
- Compute the moving average of the last 3 temperature readings for each distinct sensor ID.
- If a sensor has fewer than 3 readings so far, compute the average of the available readings.
- The pipeline must use Bytewax's stateful processing capabilities.
- Write the output to `output.csv` in the current directory.

## Implementation Hints
- Run the dataflow using `python -m bytewax.run pipeline:flow`.
- Use a custom stateful logic function with `op.stateful_map`.
- Ensure keys are strings.
- Return a new state object (do not mutate the state in-place) to ensure compatibility with recovery snapshots.
- Use standard Python file I/O or Bytewax connectors for reading and writing CSV lines.

## Acceptance Criteria
- Project path: /home/user/bytewax-task
- Command: python -m bytewax.run pipeline:flow
- Input: `input.csv` with lines formatted as `sensor_id,temperature` (e.g., `s1,20.5`).
- Output: The pipeline must create `output.csv` with lines formatted as `sensor_id,moving_average` (e.g., `s1,20.50`).
- The moving average must be a float rounded to 2 decimal places.
- The output file must contain exactly one line per input line, representing the moving average at that point in time.
