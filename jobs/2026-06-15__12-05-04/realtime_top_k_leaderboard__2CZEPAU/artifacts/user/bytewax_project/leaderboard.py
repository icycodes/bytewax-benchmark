"""
Real-time Top-K Leaderboard using Bytewax.

Processes a stream of player score events from a JSONL file, tracks each
player's maximum score using stateful processing, then computes the global
Top-K leaderboard and writes the final result to a JSON output file.

Usage:
    python3 leaderboard.py --input <input_file> --output <output_file> --k <k>
"""

import argparse
import heapq
import json
from pathlib import Path
from typing import List, Optional, Tuple

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
import bytewax.operators as op


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real-time Top-K leaderboard using Bytewax."
    )
    parser.add_argument(
        "--input", required=True, help="Path to input JSONL file."
    )
    parser.add_argument(
        "--output", required=True, help="Path to output JSON file."
    )
    parser.add_argument(
        "--k", type=int, required=True, help="Number of top players to track."
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Stateful logic helpers
# ---------------------------------------------------------------------------

def track_max_score(
    state: Optional[float], score: float
) -> Tuple[float, float]:
    """
    Stateful mapper: keep the player's all-time highest score.

    :arg state: Current maximum score for this player, or ``None`` on first
        encounter.
    :arg score: Incoming score update.
    :returns: ``(new_state, emit_value)`` — both are the updated maximum.
    """
    new_max = score if state is None else max(state, score)
    return (new_max, new_max)


def build_topk_folder(k: int):
    """
    Return a *folder* function that maintains a min-heap of size *k*.

    The heap stores ``(score, player_id)`` tuples so that the heap root is
    always the *lowest* score among the current Top-K candidates, enabling
    O(log k) insertion.
    """

    def folder(
        heap: List[Tuple[float, str]], item: Tuple[str, float]
    ) -> List[Tuple[float, str]]:
        """
        Update the Top-K heap with a new ``(player_id, max_score)`` pair.

        Because the same player_id can arrive multiple times (once per score
        update), we replace any existing entry for that player rather than
        accumulating duplicates.
        """
        player_id, score = item

        # Remove any stale entry for this player from the heap.
        heap = [(s, pid) for s, pid in heap if pid != player_id]
        heapq.heapify(heap)

        if len(heap) < k:
            heapq.heappush(heap, (score, player_id))
        elif score > heap[0][0]:
            # New score beats the current minimum — replace it.
            heapq.heapreplace(heap, (score, player_id))

        return heap

    return folder


# ---------------------------------------------------------------------------
# Collected results (written by the output sink callback)
# ---------------------------------------------------------------------------

_collected_results: List[Tuple[str, List[Tuple[float, str]]]] = []


# ---------------------------------------------------------------------------
# Custom sink: collect results in-memory then dump to file at the end
# ---------------------------------------------------------------------------

class _MemorySink:
    """
    Minimal DynamicSink-compatible sink that appends items to a Python list.
    """

    def __init__(self, results: list):
        self._results = results

    def write_batch(self, items):
        self._results.extend(items)

    def close(self):
        pass


from bytewax.outputs import DynamicSink, StatelessSinkPartition


class _MemorySinkPartition(StatelessSinkPartition):
    def __init__(self, results: list):
        self._results = results

    def write_batch(self, items):
        self._results.extend(items)

    def close(self):
        pass


class MemoryCollectorSink(DynamicSink):
    """Bytewax DynamicSink that collects all emitted items into a list."""

    def __init__(self, results: list):
        self._results = results

    def build(self, _step_id: str, _worker_index: int, _worker_count: int):
        return _MemorySinkPartition(self._results)


# ---------------------------------------------------------------------------
# Dataflow construction
# ---------------------------------------------------------------------------

def build_flow(input_path: str, k: int, results: list) -> Dataflow:
    """
    Construct and return the Bytewax dataflow.

    Pipeline stages
    ---------------
    1. **Input** — Read JSONL lines from ``input_path``.
    2. **Parse** — Deserialise each line into ``(player_id, score)``.
    3. **Key** — Use ``player_id`` as the stream key.
    4. **Max score per player** — ``stateful_map`` keeps each player's
       all-time best score; emits ``(player_id, current_max)`` on every
       update.
    5. **Reroute to single key** — Map all items to the constant key
       ``"__global__"`` so the next stateful step sees the full picture.
    6. **Top-K accumulation** — ``fold_final`` builds a min-heap of size *k*
       over the entire stream; emits once at EOF.
    7. **Serialise** — Convert the heap to a sorted list of dicts.
    8. **Output** — Write to the in-memory collector sink.
    """
    flow = Dataflow("leaderboard")

    # 1. Input: read lines from the JSONL file
    raw = op.input("file_input", flow, FileSource(input_path))

    # 2. Parse each JSON line → (player_id, score)
    def parse_line(line: str) -> Optional[Tuple[str, float]]:
        line = line.strip()
        if not line:
            return None
        record = json.loads(line)
        return (record["player_id"], float(record["score"]))

    parsed = op.filter_map("parse", raw, parse_line)

    # 3. Key the stream by player_id so stateful_map can track per-player state
    keyed = op.key_on("key_by_player", parsed, lambda pair: pair[0])

    # 4. Stateful max per player: state = current max score, emit updated max
    def max_mapper(state: Optional[float], pair: Tuple[str, float]):
        _player_id, score = pair
        new_max = score if state is None else max(state, score)
        return (new_max, new_max)

    player_max = op.stateful_map("player_max_score", keyed, max_mapper)
    # player_max stream: (player_id, current_max_score)

    # 5. Re-key everything to a single global key to aggregate Top-K
    def to_global_key(item: Tuple[str, float]) -> Tuple[str, Tuple[str, float]]:
        player_id, max_score = item
        return ("__global__", (player_id, max_score))

    globally_keyed = op.map("to_global_key", player_max, to_global_key)

    # 6. Fold into a Top-K min-heap; emitted once at EOF
    topk_stream = op.fold_final(
        "topk_fold",
        globally_keyed,
        list,                       # builder: start with an empty list
        build_topk_folder(k),       # folder: heap-maintain Top-K
    )
    # topk_stream: ("__global__", [(score, player_id), ...])

    # 7. Serialise: sort descending and convert to list of dicts
    def serialise_topk(item: Tuple[str, list]) -> str:
        _key, heap = item
        top_players = sorted(heap, key=lambda x: x[0], reverse=True)
        result = [{"player_id": pid, "score": score} for score, pid in top_players]
        return json.dumps(result)

    serialised = op.map("serialise", topk_stream, serialise_topk)

    # 8. Output to in-memory collector
    op.output("collect_output", serialised, MemoryCollectorSink(results))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    results: List[str] = []
    flow = build_flow(args.input, args.k, results)

    run_main(flow)

    # The fold_final step emits exactly one item (the final heap serialised as
    # a JSON string).  Write it to the output file.
    if results:
        output_json = results[-1]   # take the last (and only) emission
    else:
        # Edge case: empty input — produce an empty leaderboard
        output_json = json.dumps([])

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_json)
    print(f"Top-{args.k} leaderboard written to {output_path}")


if __name__ == "__main__":
    main()
