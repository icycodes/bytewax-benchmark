import argparse
import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()

def state_machine(state, event):
    if state is None:
        state = {"state": "LOGGED_OUT", "login_ts": None, "large_tx_count": 0}
    
    new_state = state.copy()
    event_type = event["event_type"]
    ts = event["timestamp"]
    
    if event_type == "login":
        new_state["state"] = "LOGGED_IN"
        new_state["login_ts"] = ts
        new_state["large_tx_count"] = 0
        return new_state, []
        
    elif event_type == "logout":
        new_state["state"] = "LOGGED_OUT"
        new_state["login_ts"] = None
        new_state["large_tx_count"] = 0
        return new_state, []
        
    elif event_type == "transaction":
        if new_state["state"] in ("LOGGED_IN", "SUSPICIOUS"):
            if ts - new_state["login_ts"] > 300:
                new_state["state"] = "LOGGED_OUT"
                new_state["login_ts"] = None
                new_state["large_tx_count"] = 0
                return new_state, []
            
            amount = event.get("amount", 0)
            if amount >= 1000:
                new_state["large_tx_count"] += 1
                new_state["state"] = "SUSPICIOUS"
                
                if new_state["large_tx_count"] >= 3:
                    new_state["state"] = "LOGGED_OUT"
                    new_state["login_ts"] = None
                    new_state["large_tx_count"] = 0
                    return new_state, ["FRAUD_ALERT"]
                    
        return new_state, []
    
    return new_state, []

def format_output(key_alert):
    key, alert = key_alert
    return key, json.dumps({"user_id": key, "alert": alert})

def main():
    args = parse_args()
    
    flow = Dataflow("fraud_detection")
    
    # Read from input file
    inp = op.input("inp", flow, FileSource(args.input))
    
    # Parse JSON
    parsed = op.map("parse_json", inp, json.loads)
    
    # Key by user_id
    keyed = op.key_on("key_on_user", parsed, lambda x: x["user_id"])
    
    # Apply state machine
    processed = op.stateful_map("fraud_state_machine", keyed, state_machine)
    
    # Flatten the alerts
    alerts = op.flat_map_value("flatten_alerts", processed, lambda x: x)
    
    # Format to JSON strings, keeping the key
    formatted = op.map("format_output", alerts, format_output)
    
    # Write to output file
    op.output("out", formatted, FileSink(args.output))
    
    run_main(flow)

if __name__ == "__main__":
    main()
