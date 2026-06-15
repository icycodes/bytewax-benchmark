# Tumbling Window OHLCV Aggregation

## Background
Financial market data often arrives as a continuous stream of individual trades. To analyze this data, it's common to aggregate trades into Open-High-Low-Close-Volume (OHLCV) bars over fixed time intervals using a stream processing framework like Bytewax.

## Requirements
- Create a Bytewax dataflow that reads a CSV file containing trade data.
- Group the trades by ticker symbol.
- Apply a 1-minute tumbling window using the trade timestamps (event time).
- For each window and symbol, calculate the OHLCV metrics: Open (first trade price), High (maximum trade price), Low (minimum trade price), Close (last trade price), and Volume (sum of trade volumes).
- Write the aggregated OHLCV bars to a JSON Lines (JSONL) file.

## Implementation Hints
- Use Bytewax's `EventClock` and `TumblingWindower` to define the time-based windowing logic.
- The input stream should be keyed by the ticker symbol before windowing.
- Consider using `fold_window` to accumulate the OHLCV state as each trade arrives in the window.
- Ensure the output records contain the window start time, symbol, and the calculated open, high, low, close, and volume values.

## Acceptance Criteria
- Project path: /home/user/myproject
- Command: `python run.py --input input.csv --output output.jsonl`
- Input format: CSV with header `timestamp,symbol,price,volume`. Timestamps are in ISO 8601 format.
- Output format: JSONL file where each line is a JSON object with keys: `window_start` (ISO 8601), `symbol`, `open`, `high`, `low`, `close`, `volume`.
- The output must accurately reflect the 1-minute tumbling window aggregations aligned to the minute.

