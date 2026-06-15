import argparse
import json
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource, TestingSink

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()

def run():
    args = parse_args()
    
    # Read input file
    with open(args.input, "r") as f:
        lines = f.readlines()
        
    events = [json.loads(line) for line in lines]
    
    flow = Dataflow("rate_limiter")
    
    # Create source
    stream = op.input("input", flow, TestingSource(events))
    
    # Key on user_id
    keyed_stream = op.key_on("key_on_user", stream, lambda x: str(x["user_id"]))
    
    # Stateful map for rate limiting
    MAX_CAPACITY = 10.0
    REFILL_RATE = 2.0
    
    def rate_limit_mapper(state, event):
        # state is (current_tokens, last_timestamp)
        # if state is None, initialize it
        if state is None:
            # We assume initial state is full capacity, but wait - 
            # should we refill before the first event? No, it starts full.
            # But what if the cost is applied immediately?
            # Let's say state before first event is full capacity.
            # We don't need to refill before the first event, because time elapsed from... itself is 0.
            # So initialize state with MAX_CAPACITY and current event's timestamp.
            state = (MAX_CAPACITY, event["timestamp"])
            
        current_tokens, last_timestamp = state
        
        # calculate time elapsed
        time_elapsed = event["timestamp"] - last_timestamp
        
        # refill tokens
        current_tokens += time_elapsed * REFILL_RATE
        if current_tokens > MAX_CAPACITY:
            current_tokens = MAX_CAPACITY
            
        cost = event["cost"]
        
        if cost == -1:
            current_tokens = MAX_CAPACITY
            event["allowed"] = True
        elif current_tokens >= cost:
            current_tokens -= cost
            event["allowed"] = True
        else:
            event["allowed"] = False
            
        # return new state and the modified event
        return (current_tokens, event["timestamp"]), event
        
    processed_stream = op.stateful_map("rate_limit", keyed_stream, rate_limit_mapper)
    
    # We only care about the values, not the keys anymore
    # processed_stream outputs (key, event)
    events_only = op.map("drop_key", processed_stream, lambda x: x[1])
    
    # Sink to a list
    output_events = []
    op.output("output", events_only, TestingSink(output_events))
    
    run_main(flow)
    
    # Write output to file
    with open(args.output, "w") as f:
        for event in output_events:
            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
    run()
