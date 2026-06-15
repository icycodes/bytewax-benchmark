import argparse
import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main

def main():
    parser = argparse.ArgumentParser(description="Bytewax Stateful Rate Limiter")
    parser.add_argument("--input", required=True, help="Input JSONL file path")
    parser.add_argument("--output", required=True, help="Output JSONL file path")
    args = parser.parse_args()
    
    # Read input JSONL file
    input_events = []
    with open(args.input, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                input_events.append(json.loads(line))
                
    # Build dataflow
    flow = Dataflow("rate_limiter_flow")
    
    # Input step
    stream = op.input("input_step", flow, TestingSource(input_events))
    
    # Key on user_id (ensure keys are strictly Python strings)
    keyed_stream = op.key_on("key_step", stream, lambda x: str(x["user_id"]))
    
    # Stateful map step
    def rate_limit_mapper(state, event):
        user_id = event["user_id"]
        timestamp = event["timestamp"]
        cost = event["cost"]
        
        max_capacity = 10.0
        refill_rate = 2.0
        
        if state is None:
            # Initial state: Full capacity (10.0 tokens)
            current_tokens = max_capacity
            last_timestamp = timestamp
        else:
            current_tokens, last_timestamp = state
            
        elapsed = max(0.0, timestamp - last_timestamp)
        refill = elapsed * refill_rate
        tokens = min(max_capacity, current_tokens + refill)
        
        if cost == -1:
            new_tokens = max_capacity
            allowed = True
        elif tokens >= cost:
            new_tokens = tokens - cost
            allowed = True
        else:
            new_tokens = tokens
            allowed = False
            
        new_state = (new_tokens, timestamp)
        
        output_event = {
            "user_id": user_id,
            "timestamp": timestamp,
            "cost": cost,
            "allowed": allowed
        }
        
        return (new_state, output_event)
        
    processed_keyed_stream = op.stateful_map("rate_limit_step", keyed_stream, rate_limit_mapper)
    
    # Remove keys
    output_stream = op.key_rm("remove_key_step", processed_keyed_stream)
    
    # Output step
    output_events = []
    op.output("output_step", output_stream, TestingSink(output_events))
    
    # Run dataflow
    run_main(flow)
    
    # Write to output JSONL file
    with open(args.output, "w") as f:
        for event in output_events:
            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
    main()
