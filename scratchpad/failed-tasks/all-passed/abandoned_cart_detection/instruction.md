# Bytewax Abandoned Cart Detection

## Background
E-commerce platforms need to detect when users abandon their shopping carts to send follow-up emails. You will build a stateful stream processing pipeline using Bytewax to detect abandoned carts based on time expiration.

## Requirements
- Create a Bytewax dataflow in `cart_pipeline.py`.
- Read JSON-encoded events line-by-line from a file specified by the `INPUT_FILE` environment variable.
- Event formats:
  - Add to cart: `{"user_id": "u1", "type": "add_to_cart", "item": "laptop"}`
  - Checkout: `{"user_id": "u1", "type": "checkout"}`
- Maintain the state of each user's cart (a list of items).
- If a cart remains unpaid for `CART_TIMEOUT_SECONDS` (read from env, default 900) after the *first* item is added, it is considered abandoned.
- When a cart is abandoned, emit a JSON object to a file specified by the `OUTPUT_FILE` environment variable: `{"user_id": "u1", "abandoned_items": ["laptop"]}` and clear the user's cart state.
- If a `checkout` event occurs before the timeout, clear the cart state (do not emit an abandonment event).
- If the stream reaches EOF, any remaining non-empty carts must immediately be emitted as abandoned.
- You must use a custom stateful operator (e.g., `StatefulLogic` or equivalent stateful timeout mechanism in Bytewax) to manage the cart state and time-based expiration.

## Implementation Hints
- Parse the input file and key the stream by `user_id`.
- Implement a custom stateful logic class to maintain the items and track the expiration time.
- Use the `notify_at` and `on_notify` methods (or equivalent) to handle time-based expiration.
- Handle the `on_eof` method to flush pending carts when the input stream ends.
- Write the results to the output file using a standard Bytewax sink.

## Acceptance Criteria
- Project path: `/home/user/project`
- Command: `python -m bytewax.run cart_pipeline:flow`
- The input file is specified by `INPUT_FILE`.
- The output file is specified by `OUTPUT_FILE`.
- The timeout is specified by `CART_TIMEOUT_SECONDS`.
- The output file must contain exactly one JSON object per line for each abandoned cart.
- Carts that are checked out must NOT be in the output.
- Carts that time out must be in the output.
- Carts pending at EOF must be in the output.

