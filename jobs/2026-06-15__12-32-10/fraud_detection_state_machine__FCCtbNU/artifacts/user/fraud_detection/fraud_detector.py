"""Fraud detection state machine using Bytewax."""

import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink


def parse_event(line):
    """Parse a JSON line into an event dict."""
    return json.loads(line)


def fraud_detector(state, event):
    """Stateful fraud detection state machine per user.

    States: LOGGED_OUT, LOGGED_IN, SUSPICIOUS

    Transitions:
      - login: -> LOGGED_IN (record timestamp, reset counter)
      - transaction with amount >= 1000 while LOGGED_IN/SUSPICIOUS
        and within 300s of login: increment counter, -> SUSPICIOUS
        (if counter >= 3: emit FRAUD_ALERT, -> LOGGED_OUT)
      - transaction > 300s after login: -> LOGGED_OUT (ignore transaction)
      - logout: -> LOGGED_OUT

    Returns (new_state, alert_or_none).
    """
    if state is None:
        state = {
            "status": "LOGGED_OUT",
            "login_time": None,
            "large_tx_count": 0,
        }

    user_id = event["user_id"]
    event_type = event["event_type"]
    amount = event.get("amount", 0)
    timestamp = event["timestamp"]

    alert = None

    if event_type == "login":
        state = {
            "status": "LOGGED_IN",
            "login_time": timestamp,
            "large_tx_count": 0,
        }
    elif event_type == "logout":
        state = {
            "status": "LOGGED_OUT",
            "login_time": None,
            "large_tx_count": 0,
        }
    elif event_type == "transaction":
        if state["status"] in ("LOGGED_IN", "SUSPICIOUS"):
            if timestamp - state["login_time"] > 300:
                # More than 300 seconds since login: reset state, ignore transaction
                state = {
                    "status": "LOGGED_OUT",
                    "login_time": None,
                    "large_tx_count": 0,
                }
            elif amount >= 1000:
                new_count = state["large_tx_count"] + 1
                if new_count >= 3:
                    # Fraud detected: 3 large transactions within 300s of login
                    alert = {"user_id": user_id, "alert": "FRAUD_ALERT"}
                    state = {
                        "status": "LOGGED_OUT",
                        "login_time": None,
                        "large_tx_count": 0,
                    }
                else:
                    state = {
                        "status": "SUSPICIOUS",
                        "login_time": state["login_time"],
                        "large_tx_count": new_count,
                    }
            # else: small transaction within window, no state change

    return (state, alert)


def build_flow(input_path, output_path):
    """Build the fraud detection dataflow.

    Args:
        input_path: Path to input JSONlines file.
        output_path: Path to output JSONlines file.

    Returns:
        Configured Dataflow object.
    """
    flow = Dataflow("fraud_detection")

    # Read input file line by line
    inp = op.input("read_input", flow, FileSource(input_path))

    # Parse each line as JSON
    events = op.map("parse_json", inp, parse_event)

    # Key events by user_id for stateful processing
    keyed = op.key_on("key_by_user", events, lambda e: e["user_id"])

    # Run the fraud detection state machine per user
    alerts = op.stateful_map("detect_fraud", keyed, fraud_detector)

    # Filter out None values (no alert emitted)
    filtered = op.filter("filter_alerts", alerts, lambda x: x[1] is not None)

    # Format as (user_id, json_string) for the partitioned file sink
    # The sink requires (key, value) tuples for partition routing
    output = op.map("format_output", filtered, lambda x: (x[0], json.dumps(x[1])))

    # Write output to file
    op.output("write_output", output, FileSink(output_path))

    return flow