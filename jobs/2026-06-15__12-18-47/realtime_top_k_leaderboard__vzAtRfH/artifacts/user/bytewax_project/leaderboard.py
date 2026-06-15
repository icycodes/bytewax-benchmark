import argparse
import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import run_main
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

class TopKSinkPartition(StatelessSinkPartition):
    def __init__(self, output_file):
        self.output_file = output_file
        self.last_state = {}

    def write_batch(self, items):
        for item in items:
            # item is ("global", state_dict)
            self.last_state = item[1]

    def close(self):
        out_list = [{"player_id": k, "score": v} for k, v in self.last_state.items()]
        out_list.sort(key=lambda x: x["score"], reverse=True)
        with open(self.output_file, 'w') as f:
            json.dump(out_list, f)

class TopKSink(DynamicSink):
    def __init__(self, output_file):
        self.output_file = output_file

    def build(self, step_id, worker_index, worker_count):
        return TopKSinkPartition(self.output_file)

def update_max(current_max, new_score):
    if current_max is None:
        current_max = new_score
    else:
        current_max = max(current_max, new_score)
    return current_max, current_max

def make_update_topk(k):
    def update_topk(state, player_score):
        if state is None:
            state = {}
        player_id, score = player_score
        state[player_id] = score
        
        sorted_players = sorted(state.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_players) > k:
            sorted_players = sorted_players[:k]
            
        state = dict(sorted_players)
        return state, state
    return update_topk

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--k", type=int, required=True)
    args = parser.parse_args()

    flow = Dataflow("leaderboard")
    
    # Read lines from input file
    lines = op.input("in", flow, FileSource(args.input))
    
    # Parse JSON
    events = op.map("parse", lines, lambda line: json.loads(line))
    
    # Key by player_id
    keyed_events = op.map("key_by_player", events, lambda ev: (ev["player_id"], ev["score"]))
    
    # Stateful map to keep max score per player
    player_max = op.stateful_map("player_max", keyed_events, update_max)
    
    # Map to single global key
    global_keyed = op.map("global_key", player_max, lambda x: ("global", x))
    
    # Stateful map to maintain Top-K
    top_k_stream = op.stateful_map("top_k", global_keyed, make_update_topk(args.k))
    
    # Output using custom sink
    op.output("out", top_k_stream, TopKSink(args.output))
    
    # Run the dataflow
    run_main(flow)

if __name__ == "__main__":
    main()
