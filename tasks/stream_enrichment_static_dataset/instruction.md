# Stream Enrichment and Windowed Aggregation

## Background
Build a stateful stream processing pipeline using Bytewax that enriches a stream of transactions with a large static dataset of product metadata, and calculates revenue per category over time.

## Requirements
- Read a static dataset of product metadata from `products.json` at initialization.
- Ingest a stream of transaction events from `transactions.jsonl`.
- Enrich each transaction with `category` and `price` from the product metadata.
- Drop transactions if their `product_id` is not found in the static dataset.
- Group the enriched transactions by `category` and calculate the total revenue (`price * quantity`) using a 1-minute tumbling window based on the event's `timestamp` (event time). Align the windows to the start of the hour.
- Output the aggregated revenue per category for each window to standard output.

## Implementation Hints
- Load the static dataset in the global scope or inside a stateless mapper.
- Use `bytewax.operators.windowing` for the event-time tumbling window.
- Ensure the timestamp is parsed into a timezone-aware `datetime` object for the `EventClock`.
- The output of the windowing operator needs to be formatted into the required JSON string before being passed to `StdOutSink`.

## Acceptance Criteria
- Project path: /home/user/bytewax_project
- Command: `python -m bytewax.run enrichment:flow`
- Input files (must be read from the current directory):
  - `products.json`: JSON object mapping `product_id` to `{"category": string, "price": number}`.
  - `transactions.jsonl`: JSON lines file where each line is `{"transaction_id": string, "product_id": string, "timestamp": "YYYY-MM-DDTHH:MM:SSZ", "quantity": number}`.
- Expected Output:
  - Standard output must contain JSON strings (one per line) representing the windowed aggregation results.
  - Output format: `{"category": "<category_name>", "window_start": "<ISO8601_timestamp>", "revenue": <total_revenue>}`.
  - The `window_start` should be the start time of the tumbling window (e.g., `2026-01-01T12:00:00+00:00` or `2026-01-01T12:00:00Z`).

