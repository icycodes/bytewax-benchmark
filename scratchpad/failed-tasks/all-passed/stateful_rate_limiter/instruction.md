# Bytewax Stateful Rate Limiter

## Background
Implement a per-user Token Bucket rate limiter using Bytewax's stateful stream processing capabilities. The task requires building a dataflow that processes a stream of user requests, applies rate limiting logic using `stateful_map`, and outputs whether each request is allowed or rejected.

## Requirements
- Create a Python script `rate_limiter.py` that processes an input JSONL file and writes to an output JSONL file.
- The script must use Bytewax to build and execute the dataflow synchronously (e.g., using `bytewax.testing.run_main`).
- Implement a Token Bucket algorithm per `user_id` with:
  - Maximum capacity: 10.0 tokens
  - Refill rate: 2.0 tokens per second
  - Initial state: Full capacity (10.0 tokens)
- The token bucket state must be maintained using Bytewax's `op.stateful_map`.
- For each event, calculate the tokens refilled since the last event's timestamp, cap at maximum capacity, and determine if the requested `cost` can be fulfilled.
- If the bucket has enough tokens, deduct the `cost` and mark `allowed: true`. Otherwise, do not deduct tokens and mark `allowed: false`.
- Support a special `cost` of `-1` which resets the user's bucket to full capacity and always evaluates to `allowed: true`.

## Implementation Hints
- Read input arguments using `argparse` to get `--input` and `--output` file paths.
- Use a custom source (like `bytewax.testing.TestingSource` over the read lines) or a basic file input connector to ingest the JSONL data.
- Parse the JSON lines, then use `op.key_on` to convert the stream into a Keyed Stream `(user_id_string, event_dict)`.
- In Bytewax v0.21+, `stateful_map` does not take a `builder` argument. Your mapper function will receive `None` as the state for the very first event of a key, and you must initialize the state (e.g., a tuple of `(current_tokens, last_timestamp)`) inside the mapper.
- Ensure that keys are strictly Python strings, as required by Bytewax's routing engine.
- Write the results out to the specified output file as JSON lines. You can use a custom sink or map the output to string and write it out.

## Acceptance Criteria
- Project path: `/home/user/bytewax-rate-limiter`
- Command: `python rate_limiter.py --input <input_file> --output <output_file>`
- Input format: A JSONL file where each line is `{"user_id": string, "timestamp": float, "cost": float}`
- Output format: A JSONL file where each line is `{"user_id": string, "timestamp": float, "cost": float, "allowed": boolean}`
- The output file must contain exactly one line for each input line, reflecting the correct rate limiting state.

