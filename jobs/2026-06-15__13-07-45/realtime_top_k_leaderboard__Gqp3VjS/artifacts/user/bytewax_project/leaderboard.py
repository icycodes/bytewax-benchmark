"""
Real-time Top-K Leaderboard using Bytewax.

Processes a stream of player score events, maintains the max score per player,
and computes the global Top-K leaderboard.
"""

import argparse
import heapq
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import run_main

import bytewax.operators as op


# ---------------------------------------------------------------------------
# Input: JSONL file source
# ---------------------------------------------------------------------------

class JSONLSourcePartition(StatelessSourcePartition):
    """Reads JSON lines from a file, one per batch."""

    def __init__(self, path: str):
        self._path = path
        self._file = None

    def next_batch(self):
        if self._file is None:
            self._file = open(self._path, "r")
        line = self._file.readline()
        if not line:
            raise StopIteration()
        record = json.loads(line.strip())
        return [record]

    def close(self):
        if self._file is not None:
            self._file.close()


class JSONLSource(DynamicSource):
    """Dynamic source that reads a JSONL file."""

    def __init__(self, path: str):
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        # Only the first worker reads the file to avoid duplicates.
        if worker_index == 0:
            return JSONLSourcePartition(self._path)
        else:
            # Return a partition that immediately stops.
            return _EmptySourcePartition()


class _EmptySourcePartition(StatelessSourcePartition):
    """A source partition that produces nothing."""

    def next_batch(self):
        raise StopIteration()


# ---------------------------------------------------------------------------
# Output: JSON file sink
# ---------------------------------------------------------------------------

class JSONFileSinkPartition(StatelessSinkPartition):
    """Writes items to a JSON file."""

    def __init__(self, path: str):
        self._path = path
        self._items: List[dict] = []

    def write_batch(self, items):
        self._items.extend(items)

    def close(self):
        with open(self._path, "w") as f:
            json.dump(self._items, f)


class JSONFileSink(DynamicSink):
    """Dynamic sink that writes to a JSON file."""

    def __init__(self, path: str):
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        # Only worker 0 writes the output.
        if worker_index == 0:
            return JSONFileSinkPartition(self._path)
        else:
            return _NoopSinkPartition()


class _NoopSinkPartition(StatelessSinkPartition):
    """A sink partition that discards items."""

    def write_batch(self, items):
        pass


# ---------------------------------------------------------------------------
# Stateful logic: max-score-per-player
# ---------------------------------------------------------------------------

def max_score_mapper(
    current_max: Optional[float], event: dict
) -> Tuple[Optional[float], dict]:
    """Stateful mapper: keep the max score for each player.

    Args:
        current_max: The current max score for this player (None if first time).
        event: The incoming event dict with 'player_id' and 'score'.

    Returns:
        (updated_max, output_event) — the output event has the player's
        best score so far.
    """
    score = event["score"]
    if current_max is None or score > current_max:
        new_max = score
    else:
        new_max = current_max

    return (new_max, {"player_id": event["player_id"], "score": new_max})


# ---------------------------------------------------------------------------
# Global Top-K: maintained via fold_final
# ---------------------------------------------------------------------------

def build_topk(k: int):
    """Factory that returns a builder/folder pair for fold_final.

    The accumulator is a min-heap (list) of (-score, player_id) tuples,
    limited to size k.  Using negative score makes it a max-heap via
    heapq's default min-heap behaviour when popping the smallest.
    """

    def builder() -> List[Tuple[float, str]]:
        return []  # empty heap

    def folder(
        heap: List[Tuple[float, str]], event: dict
    ) -> List[Tuple[float, str]]:
        score = event["score"]
        player_id = event["player_id"]

        # Check if this player is already in the heap with a lower score
        found = False
        for i, (s, pid) in enumerate(heap):
            if pid == player_id:
                if -s < score:  # new score is higher
                    heap[i] = (-score, player_id)
                    heapq.heapify(heap)
                found = True
                break

        if not found:
            heapq.heappush(heap, (-score, player_id))
            if len(heap) > k:
                heapq.heappop(heap)  # remove smallest (lowest score)

        return heap

    return builder, folder


# ---------------------------------------------------------------------------
# Dataflow construction
# ---------------------------------------------------------------------------

def build_flow(input_path: str, output_path: str, k: int) -> Dataflow:
    """Construct the Bytewax dataflow for the leaderboard."""

    flow = Dataflow("leaderboard")

    # 1. Read JSONL input
    inp = op.input("read_input", flow, JSONLSource(input_path))

    # 2. Key by player_id so stateful_map can track per-player max
    keyed = op.key_on("key_by_player", inp, lambda e: e["player_id"])

    # 3. Stateful map: maintain max score per player
    maxed = op.stateful_map("max_per_player", keyed, max_score_mapper)

    # 4. Route all updates to a single key for global Top-K aggregation.
    #    First remove the existing player_id key, then re-key to a constant key
    #    so all updates land on the same worker for global aggregation.
    unkeyed = op.key_rm("remove_player_key", maxed)
    global_keyed = op.key_on("key_to_global", unkeyed, lambda _: "global_leaderboard")

    # 5. fold_final: accumulate Top-K.  Only emits at EOF (perfect for us).
    builder, folder = build_topk(k)
    topk_stream = op.fold_final("topk_aggregate", global_keyed, builder, folder)

    # 6. Transform the heap into the sorted output format
    def format_output(key__heap: Tuple[str, List[Tuple[float, str]]]) -> List[dict]:
        _key, heap = key__heap
        # Convert heap to sorted list (descending by score)
        result = []
        for neg_score, player_id in heap:
            result.append({"player_id": player_id, "score": -neg_score})
        result.sort(key=lambda x: x["score"], reverse=True)
        return result

    formatted = op.flat_map("format_output", topk_stream, format_output)

    # 7. Write to output JSON file
    op.output("write_output", formatted, JSONFileSink(output_path))

    return flow


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Real-time Top-K Leaderboard with Bytewax"
    )
    parser.add_argument(
        "--input", required=True, help="Path to input JSONL file"
    )
    parser.add_argument(
        "--output", required=True, help="Path to output JSON file"
    )
    parser.add_argument(
        "--k", type=int, required=True, help="Number of top players to output (K)"
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    flow = build_flow(args.input, args.output, args.k)
    run_main(flow)


if __name__ == "__main__":
    main()
