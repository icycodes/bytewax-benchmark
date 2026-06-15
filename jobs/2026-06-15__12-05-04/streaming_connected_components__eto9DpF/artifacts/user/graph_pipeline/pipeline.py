"""
Streaming Connected Components with Bytewax v0.21.1

Reads edges from input_edges.jsonl, maintains a Union-Find (Disjoint Set)
data structure via stateful_map, and writes per-edge events to
output_events_${ZEALT_RUN_ID}.jsonl.

Run with SQLite recovery:
    python -m bytewax.recovery recovery_db 1
    ZEALT_RUN_ID=<id> python -m bytewax.run pipeline:flow \
        -r recovery_db -s 10
"""

import json
import os
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Union-Find (Disjoint Set Union) — fully picklable pure-Python class
# ---------------------------------------------------------------------------

class UnionFind:
    """Path-compressed, union-by-size disjoint-set data structure."""

    def __init__(self) -> None:
        # parent[node] = representative of its component
        self.parent: dict[str, str] = {}
        # size[root] = number of nodes in the component
        self.size: dict[str, int] = {}

    def _make(self, node: str) -> None:
        """Register a new node as its own component if not seen yet."""
        if node not in self.parent:
            self.parent[node] = node
            self.size[node] = 1

    def find(self, node: str) -> str:
        """Return the root representative of *node* with path compression."""
        self._make(node)
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression
        cur = node
        while cur != root:
            nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    def union(self, u: str, v: str) -> Tuple[bool, int]:
        """
        Union the components containing *u* and *v*.

        Returns:
            (merged: bool, new_size: int)
            merged is True when the two nodes were in different components.
        """
        ru, rv = self.find(u), self.find(v)
        if ru == rv:
            return False, self.size[ru]
        # Union by size — attach smaller tree under larger
        if self.size[ru] < self.size[rv]:
            ru, rv = rv, ru
        self.parent[rv] = ru
        self.size[ru] += self.size[rv]
        return True, self.size[ru]


# ---------------------------------------------------------------------------
# Dataflow construction
# ---------------------------------------------------------------------------

# Resolve run-id and output path at module load time so the dataflow object
# (which Bytewax imports) is fully configured before execution begins.
run_id = os.environ.get("ZEALT_RUN_ID", "default")
output_path = Path(__file__).parent / f"output_events_{run_id}.jsonl"

# Ensure the output file exists (FileSink requires it to be present).
output_path.touch()

flow = Dataflow("connected_components")

# 1. Read each line of the edge file as a raw string.
raw = op.input(
    "read_edges",
    flow,
    FileSource(Path(__file__).parent / "input_edges.jsonl"),
)

# 2. Parse JSON → dict with keys "u" and "v".
edges = op.map(
    "parse_json",
    raw,
    lambda line: json.loads(line),
)

# 3. Route ALL edges to a single partition ("global") so that the Union-Find
#    state covers the entire graph, not just a shard of it.
keyed_edges = op.key_on(
    "single_partition",
    edges,
    lambda _edge: "global",
)

# 4. Stateful mapper: maintain one UnionFind instance per key ("global").
def uf_mapper(
    state: Optional[UnionFind],
    edge: dict,
) -> Tuple[Optional[UnionFind], str]:
    """
    Apply one edge to the Union-Find state and produce an output event.

    Returns (new_state, json_string) — the JSON string is what we write.
    """
    if state is None:
        state = UnionFind()

    u, v = edge["u"], edge["v"]
    merged, component_size = state.union(u, v)

    if merged:
        event = {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": component_size,
        }
    else:
        event = {
            "u": u,
            "v": v,
            "status": "already_connected",
            "component_size": component_size,
        }

    return state, json.dumps(event)

keyed_events = op.stateful_map(
    "union_find",
    keyed_edges,
    uf_mapper,
)

# 5. FileSink (a FixedPartitionedSink) expects (key, value) 2-tuples where
#    the value is a plain string.  The keyed_events stream already carries
#    ("global", json_string) tuples from stateful_map, so we can feed it
#    directly into the sink without stripping the key.
op.output(
    "write_events",
    keyed_events,
    FileSink(output_path),
)
