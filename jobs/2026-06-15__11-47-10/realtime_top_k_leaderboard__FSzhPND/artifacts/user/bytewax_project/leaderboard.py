import argparse
import json
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main


def parse_json(line: str) -> Optional[Tuple[str, float]]:
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        if "player_id" in data and "score" in data:
            return (str(data["player_id"]), float(data["score"]))
    except Exception:
        pass
    return None


def update_max_score(current_max: Optional[float], score: float) -> Tuple[Optional[float], float]:
    if current_max is None:
        current_max = score
    else:
        current_max = max(current_max, score)
    return current_max, current_max


def make_update_top_k(k: int):
    def update_top_k(state: Optional[Dict[str, float]], item: Tuple[str, float]) -> Tuple[Optional[Dict[str, float]], List[Dict[str, Any]]]:
        if state is None:
            state = {}
        
        player_id, score = item
        
        if player_id in state:
            state[player_id] = max(state[player_id], score)
        else:
            if len(state) < k:
                state[player_id] = score
            else:
                min_player = min(state, key=state.get)
                min_score = state[min_player]
                if score > min_score:
                    del state[min_player]
                    state[player_id] = score
        
        # Sort the top-K list in descending order by score
        sorted_top_k = sorted(state.items(), key=lambda x: x[1], reverse=True)
        emitted_list = [{"player_id": p, "score": s} for p, s in sorted_top_k]
        return state, emitted_list
    return update_top_k


class LeaderboardSinkPartition(StatelessSinkPartition):
    def __init__(self, output_path: str, worker_index: int):
        self.output_path = output_path
        self.worker_index = worker_index
        self.last_top_k = []
        self.has_updates = False

    def write_batch(self, items: List[Tuple[str, List[Dict[str, Any]]]]):
        for item in items:
            key, top_k = item
            self.last_top_k = top_k
            self.has_updates = True

    def close(self) -> None:
        if self.has_updates or self.worker_index == 0:
            Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self.output_path, "w") as f:
                json.dump(self.last_top_k, f)


class LeaderboardSink(DynamicSink):
    def __init__(self, output_path: str):
        self.output_path = output_path

    def build(self, step_id: str, worker_index: int, worker_count: int) -> LeaderboardSinkPartition:
        return LeaderboardSinkPartition(self.output_path, worker_index)


def main():
    parser = argparse.ArgumentParser(description="Real-time Top-K Leaderboard with Bytewax")
    parser.add_argument("--input", required=True, help="Path to input JSONL file")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--k", type=int, required=True, help="Number of top players to track")
    args = parser.parse_args()

    if args.k <= 0:
        raise ValueError("k must be a positive integer")

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    flow = Dataflow("leaderboard")
    
    # 1. Read input JSONL file line-by-line
    inp = op.input("inp", flow, FileSource(args.input))
    
    # 2. Parse JSON lines and filter out invalid/empty lines
    parsed = op.filter_map("parse_json", inp, parse_json)
    
    # 3. Track the highest score ever achieved by each player
    max_scores = op.stateful_map("track_max_score", parsed, update_max_score)
    
    # 4. Route the updated player maximum scores to a single worker
    routed = op.map("route_to_global", max_scores, lambda x: ("TOP_K", x))
    
    # 5. Maintain global Top-K state
    top_k = op.stateful_map("track_top_k", routed, make_update_top_k(args.k))
    
    # 6. Output the final Top-K leaderboard to the output file
    op.output("out", top_k, LeaderboardSink(args.output))
    
    # Run the dataflow
    run_main(flow)


if __name__ == "__main__":
    main()
