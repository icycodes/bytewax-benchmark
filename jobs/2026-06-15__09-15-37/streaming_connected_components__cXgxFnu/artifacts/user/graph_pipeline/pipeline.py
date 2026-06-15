"""
Streaming Connected Components with Bytewax (v0.21.1).

Reads edges from input_edges.jsonl, maintains a Union-Find (Disjoint Set)
data structure to track connected components, and writes per-edge events to
output_events_<ZEALT_RUN_ID>.jsonl.

Run with:
    ZEALT_RUN_ID=<id> python -m bytewax.run pipeline:flow \
        -r recovery/ -s 10
"""

import json
import os
from pathlib import Path
from typing import Optional, Tuple

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
import bytewax.operators as op


# ---------------------------------------------------------------------------
# Union-Find (Disjoint Set Union) — fully picklable via plain dicts
# ---------------------------------------------------------------------------

class UnionFind:
    """Path-compressed, union-by-size disjoint-set structure."""

    def __init__(self) -> None:
        # parent[node] = parent node (node itself if root)
        self.parent: dict[str, str] = {}
        # size[root] = number of nodes in the component
        self.size: dict[str, int] = {}

    # ------------------------------------------------------------------
    def _ensure(self, node: str) -> None:
        """Register *node* as its own component if not already known."""
        if node not in self.parent:
            self.parent[node] = node
            self.size[node] = 1

    def find(self, node: str) -> str:
        """Return the root of *node*'s component (with path compression)."""
        self._ensure(node)
        # Iterative path compression
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        # Compress the path
        current = node
        while self.parent[current] != root:
            nxt = self.parent[current]
            self.parent[current] = root
            current = nxt
        return root

    def union(self, a: str, b: str) -> Tuple[bool, int]:
        """
        Merge the components containing *a* and *b*.

        Returns:
            (merged, component_size) where *merged* is True when the two
            nodes were in different components before this call, and
            *component_size* is the size of the resulting component.
        """
        root_a = self.find(a)
        root_b = self.find(b)

        if root_a == root_b:
            return False, self.size[root_a]

        # Union by size — attach smaller tree under larger tree
        if self.size[root_a] < self.size[root_b]:
            root_a, root_b = root_b, root_a

        self.parent[root_b] = root_a
        self.size[root_a] += self.size[root_b]
        return True, self.size[root_a]

    def component_size(self, node: str) -> int:
        """Return the size of *node*'s component."""
        return self.size[self.find(node)]


# ---------------------------------------------------------------------------
# Stateful mapper logic
# ---------------------------------------------------------------------------

def union_find_mapper(
    state: Optional[UnionFind],
    edge_json: str,
) -> Tuple[Optional[UnionFind], str]:
    """
    Bytewax stateful_map callback.

    Parameters
    ----------
    state:
        Current UnionFind state (None on first invocation).
    edge_json:
        Raw JSON line from the source file.

    Returns
    -------
    (new_state, output_json)
    """
    if state is None:
        state = UnionFind()

    edge = json.loads(edge_json)
    u: str = edge["u"]
    v: str = edge["v"]

    merged, size = state.union(u, v)

    if merged:
        event = {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": size,
        }
    else:
        event = {
            "u": u,
            "v": v,
            "status": "already_connected",
            "component_size": size,
        }

    return state, json.dumps(event)


# ---------------------------------------------------------------------------
# Dataflow construction
# ---------------------------------------------------------------------------

# Determine output path from environment variable
_run_id = os.environ.get("ZEALT_RUN_ID", "default")
_input_path = Path(__file__).parent / "input_edges.jsonl"
_output_path = Path(__file__).parent / f"output_events_{_run_id}.jsonl"

flow = Dataflow("connected_components")

# 1. Ingest raw JSON lines from the input file
raw: op.Stream[str] = op.input("edges_in", flow, FileSource(_input_path))

# 2. Route ALL edges to the SAME stateful partition via a constant key.
#    key_on produces Stream[Tuple[key, value]].
keyed: op.Stream[Tuple[str, str]] = op.key_on(
    "key_all", raw, lambda line: "singleton"
)

# 3. Apply Union-Find logic stateflly
processed: op.Stream[Tuple[str, str]] = op.stateful_map(
    "union_find", keyed, union_find_mapper
)

# 4. Drop the routing key — we only want the JSON string value
values: op.Stream[str] = op.map_value("drop_key", processed, lambda v: v)

# 5. Write one JSON object per line to the output file
op.output("events_out", values, FileSink(_output_path))
