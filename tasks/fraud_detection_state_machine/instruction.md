# Bytewax Fraud Detection State Machine

## Background
Implement a stateful stream processing dataflow using the Bytewax framework to detect fraudulent transaction patterns based on a complex state machine.

## Requirements
- Build a Bytewax dataflow that reads user events from a JSONlines file.
- Implement a state machine to track each user's state independently using Bytewax's stateful operators.
- The state machine must implement the following logic per `user_id`:
  - A user starts in the `LOGGED_OUT` state.
  - A `login` event transitions the user to `LOGGED_IN` and records the login timestamp.
  - A `transaction` event with `amount >= 1000` while `LOGGED_IN` or `SUSPICIOUS` increments a counter and transitions the state to `SUSPICIOUS`.
  - If the user accumulates 3 such large transactions within 300 seconds (5 minutes) of their last `login` event, emit a fraud alert and transition back to `LOGGED_OUT`.
  - Any `logout` event immediately transitions the user to `LOGGED_OUT`.
  - If a transaction occurs more than 300 seconds after the login, the state should reset to `LOGGED_OUT` (and the current transaction is ignored).
- Write the emitted fraud alerts to an output JSONlines file.

## Implementation Hints
- Use `bytewax.dataflow.Dataflow` to define the processing pipeline.
- Use a stateful operator (such as `stateful_map`) to maintain the state machine for each `user_id`.
- The state must be effectively immutable as per Bytewax guidelines; return a new state object or dictionary from your state update function.
- Ensure the input is keyed by `user_id` before passing it to the stateful operator.

## Acceptance Criteria
- Project path: /home/user/fraud_detection
- Command: python run.py --input input.jsonl --output output.jsonl
- Input format: JSONlines with fields `user_id` (string), `event_type` (string: "login", "transaction", "logout"), `amount` (number, optional), `timestamp` (integer seconds).
- Output format: JSONlines with fields `user_id` (string) and `alert` (string: "FRAUD_ALERT").
- The application must correctly identify users who meet the fraud pattern and output exactly one alert per completed pattern.
- The code must use Bytewax's stateful processing capabilities.

