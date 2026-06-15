# Anomaly Detection in Streaming Data with Bytewax

## Background
You need to build a stateful stream processing pipeline using Bytewax to detect anomalies in IoT sensor data. The pipeline must be fault-tolerant and support state recovery.

## Requirements
- Read sensor readings from a JSONL file.
- Parse the data and group it by `sensor_id`.
- Apply a sliding window over the event time of the readings. The window length should be 60 seconds, and the step size should be 20 seconds.
- For each window, calculate the mean and standard deviation of the sensor values.
- Identify any reading in the window that is an outlier (value > mean + 3 * std_dev OR value < mean - 3 * std_dev). If the standard deviation is 0, there are no outliers.
- Output the detected outliers to a JSONL file.
- The pipeline MUST support Bytewax's SQLite-based recovery mechanism. Ensure that any state maintained by the pipeline is fully picklable.

## Implementation Hints
- Use `bytewax.operators.windowing` for time-based windowing.
- You will need an `EventClock` to use the timestamps in the data, and a `SlidingWindower`.
- Ensure your data sources and sinks are compatible with Bytewax's execution model and can run non-interactively.
- Remember that Bytewax requires keys to be strings for stateful operations.

## Acceptance Criteria
- Project path: /home/user/anomaly_detection
- Command: `python -m bytewax.run pipeline:flow -r ./recovery_dir -s 1 -b 0`
- Input file: `/home/user/anomaly_detection/data.jsonl`
  - Format: `{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:00Z", "value": 22.5}`
- Output file: `/home/user/anomaly_detection/anomalies.jsonl`
  - Format: `{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:00Z", "value": 25.0, "mean": 22.5, "std_dev": 0.5}`
- The pipeline must execute successfully with the recovery directory enabled, proving that the state is picklable.
- The output file must contain exactly the detected anomalies based on the sliding window calculations.

