# Real-time Top-K Leaderboard with Bytewax

## Background
In gaming and analytics, maintaining a real-time leaderboard is a common requirement. You need to build a dataflow using Bytewax that processes a continuous stream of player score updates, maintains the maximum score achieved by each player, and continuously computes the global Top-K players.

## Requirements
- Implement a Bytewax dataflow that reads player score events from an input JSONL file.
- Use stateful processing to track the highest score ever achieved by each player.
- Maintain a global state to track the top K players overall based on their highest scores.
- Output the latest global Top-K leaderboard to an output JSON file upon completion of the stream.
- The task must be implemented in a Python script named `leaderboard.py`.

## Implementation Hints
- Use `bytewax.operators.stateful_map` or similar stateful operators to maintain the maximum score per player.
- To compute the global Top-K, you will need to route the updated player maximum scores to a single worker (e.g., by mapping to a constant key) and use another stateful operator to maintain the sorted Top-K list.
- Ensure the Top-K state is updated efficiently (e.g., using a min-heap or sorted list of size K) rather than keeping all players in memory.

## Acceptance Criteria
- Project path: `/home/user/bytewax_project`
- Command: `python3 leaderboard.py --input <input_file> --output <output_file> --k <k>`
- The script must read JSON lines from `<input_file>` where each line has the format `{"player_id": string, "score": number}`.
- The script must write a single JSON array to `<output_file>` representing the final Top-K players, sorted by score in descending order. Format: `[{"player_id": string, "score": number}, ...]`.
- The script must correctly handle multiple score updates for the same player, keeping only their highest score for leaderboard consideration.

