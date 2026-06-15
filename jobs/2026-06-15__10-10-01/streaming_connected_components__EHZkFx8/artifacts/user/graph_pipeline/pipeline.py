import json
import os
from pathlib import Path

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op


class UnionFind:
    """Union-Find (Disjoint Set Union) data structure with component sizes."""

    def __init__(self):
        self.parent: dict[str, str] = {}
        self.size: dict[str, int] = {}

    def _ensure(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1

    def find(self, x: str) -> str:
        self._ensure(x)
        # Path compression
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, x: str, y: str) -> tuple[str, int | None, int | None]:
        """
        Try to union x and y.

        Returns:
            (status, old_size_or_none, new_size_or_none)
            - "already_connected": old_size_or_none = component size
            - "merged": old_size_or_none = None, new_size_or_none = new component size
        """
        rx = self.find(x)
        ry = self.find(y)

        if rx == ry:
            return ("already_connected", self.size[rx], None)

        # Union by size: attach smaller tree under larger
        if self.size[rx] < self.size[ry]:
            rx, ry = ry, rx

        self.parent[ry] = rx
        self.size[rx] += self.size[ry]
        return ("merged", None, self.size[rx])


# Constant key to route all edges to a single partition
ROUTING_KEY = "global_union_find"


def stateful_union_find(
    uf_state: UnionFind | None,
    edge: dict,
) -> tuple[UnionFind, dict]:
    """Stateful mapper: maintains UnionFind and processes each edge."""
    if uf_state is None:
        uf_state = UnionFind()

    u = edge["u"]
    v = edge["v"]
    status, old_size, new_size = uf_state.union(u, v)

    result = {"u": u, "v": v, "status": status}
    if status == "already_connected":
        result["component_size"] = old_size
    else:
        result["new_component_size"] = new_size

    return (uf_state, result)


def build_dataflow(run_id: str) -> Dataflow:
    flow = Dataflow("connected_components")

    # Input: read edges from JSONL file
    inp = op.input("edges_input", flow, FileSource(Path("input_edges.jsonl")))

    # Parse JSON lines
    parsed = op.map("parse_json", inp, json.loads)

    # Route all edges to a single key for global state
    keyed = op.key_on("route_to_single_partition", parsed, lambda _: ROUTING_KEY)

    # Stateful processing with Union-Find
    processed = op.stateful_map("union_find", keyed, stateful_union_find)

    # Serialize the value to JSON string, keeping the (key, value) tuple
    serialized = op.map(
        "serialize",
        processed,
        lambda kv: (kv[0], json.dumps(kv[1])),
    )

    # Output: write to file
    output_path = Path(f"output_events_{run_id}.jsonl")
    op.output("edges_output", serialized, FileSink(output_path))

    return flow


def get_flow() -> Dataflow:
    """Factory for Bytewax CLI: reads ZEALT_RUN_ID from environment."""
    run_id = os.environ.get("ZEALT_RUN_ID", "unknown")
    return build_dataflow(run_id)


if __name__ == "__main__":
    # For direct execution (testing), not CLI mode
    run_id = os.environ.get("ZEALT_RUN_ID", "test")
    flow = build_dataflow(run_id)

    from bytewax.testing import run_main

    run_main(flow)
