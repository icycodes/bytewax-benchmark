# Real-Time Top-K Aggregation with Bytewax

## Background
Bytewax is a Python-native stateful stream processing framework. In this task, you will build a dataflow that computes the global Top-3 most viewed products in real-time, while ensuring the pipeline state is fully recoverable.

## Requirements
- Create a Bytewax dataflow in `pipeline.py` named `flow`.
- The dataflow should read product view events from a local `input.json` file.
- It must maintain the exact total view count for each product.
- It must maintain the global Top-3 products by view count across all products.
- The pipeline state must be fully compatible with Bytewax's SQLite recovery mechanism.
- Whenever the Top-3 list changes or is updated, output the new Top-3 list to standard output.

## Implementation Hints
- You can use `op.stateful_map` twice: first to maintain the running count per product (keyed by the product ID), and second to maintain the global Top-3 list (using a single constant routing key, e.g., `"global"`).
- Remember that Bytewax requires all routing keys to be Python strings.
- To support SQLite recovery, ensure all state objects are picklable (e.g., use native Python dicts or tuples, avoid complex objects or lambdas in state).
- When updating state in `op.stateful_map`, always return a new copy or deep-copy of the state to avoid silent state corruption during Bytewax's snapshotting process.
- You can use `TestingSource` to emit the items read from `input.json`.

## Acceptance Criteria
- Project path: `/home/user/bytewax_project`
- Command: `python -m bytewax.run pipeline:flow -r ./recovery_dir -s 10 -b 0`
- Input format: The pipeline reads `input.json`, which contains a JSON array of events. Each event is a list `[product_id, increment]` (e.g., `["prod_1", 1]`).
- Output format: The stdout should print the current Top-3 products as a JSON-encoded list of `[product_id, total_count]` arrays, sorted by count in descending order. Ensure only the JSON list is printed per update (e.g., `[["prod_1", 5], ["prod_2", 3]]`).
- Recovery: The executor must initialize the SQLite recovery partitions in `./recovery_dir` before running the pipeline.

