import argparse
import json
import pathlib
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

# Transition logic function for the state machine
def update_state(state, event):
    if state is None:
        state = {
            "status": "LOGGED_OUT",
            "login_timestamp": None,
            "large_transaction_count": 0
        }
    
    event_type = event.get("event_type")
    timestamp = event.get("timestamp")
    amount = event.get("amount", 0)
    user_id = event.get("user_id")

    # Copy the state to make it effectively immutable/new as per Bytewax guidelines
    new_state = {
        "status": state["status"],
        "login_timestamp": state["login_timestamp"],
        "large_transaction_count": state["large_transaction_count"]
    }

    emit_value = None

    if event_type == "login":
        new_state["status"] = "LOGGED_IN"
        new_state["login_timestamp"] = timestamp
        new_state["large_transaction_count"] = 0

    elif event_type == "logout":
        new_state["status"] = "LOGGED_OUT"
        new_state["login_timestamp"] = None
        new_state["large_transaction_count"] = 0

    elif event_type == "transaction":
        if new_state["status"] in ("LOGGED_IN", "SUSPICIOUS"):
            login_ts = new_state["login_timestamp"]
            # Check if transaction occurs more than 300 seconds after login
            if login_ts is not None and (timestamp - login_ts > 300):
                # Reset to LOGGED_OUT and ignore current transaction
                new_state["status"] = "LOGGED_OUT"
                new_state["login_timestamp"] = None
                new_state["large_transaction_count"] = 0
            else:
                # Within 300 seconds of login
                if amount >= 1000:
                    new_state["large_transaction_count"] += 1
                    new_state["status"] = "SUSPICIOUS"
                    if new_state["large_transaction_count"] >= 3:
                        # Emit fraud alert
                        emit_value = {
                            "user_id": user_id,
                            "alert": "FRAUD_ALERT"
                        }
                        # Transition back to LOGGED_OUT
                        new_state["status"] = "LOGGED_OUT"
                        new_state["login_timestamp"] = None
                        new_state["large_transaction_count"] = 0
        else:
            # Transaction while LOGGED_OUT: ignored, keep state as is
            pass

    return (new_state, emit_value)

def main():
    parser = argparse.ArgumentParser(description="Bytewax Fraud Detection State Machine")
    parser.add_argument("--input", required=True, help="Input JSONlines file path")
    parser.add_argument("--output", required=True, help="Output JSONlines file path")
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)

    # Ensure output file parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Define Dataflow
    flow = Dataflow("fraud_detection")
    
    # Read from FileSource
    stream = op.input("inp", flow, FileSource(input_path))
    
    # Parse each line as JSON
    parsed = op.map("parse_json", stream, json.loads)
    
    # Key by user_id
    keyed = op.key_on("key_by_user", parsed, lambda x: x["user_id"])
    
    # Track stateful transitions
    stateful = op.stateful_map("fraud_detector", keyed, update_state)
    
    # Filter out None values (keep only fraud alerts)
    alerts = op.filter_value("filter_none", stateful, lambda x: x is not None)
    
    # Format as JSON string
    formatted_alerts = op.map_value("format_json", alerts, json.dumps)
    
    # Write to FileSink
    op.output("out", formatted_alerts, FileSink(output_path))
    
    # Run the dataflow
    run_main(flow)

if __name__ == "__main__":
    main()
