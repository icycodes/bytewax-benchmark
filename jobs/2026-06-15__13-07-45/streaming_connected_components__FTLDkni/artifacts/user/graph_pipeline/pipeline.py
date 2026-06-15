"""
Streaming Connected Components with Bytewax.

Processes a stream of graph edges and dynamically maintains connected
components using a Union-Find (Disjoint Set) data structure. All edges
are routed to a single stateful partition for global consistency.
"""

import json
import os
from typing import Optional, Tuple, Dict, Any

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


# ---------------------------------------------------------------------------
# Union-Find (Disjoint Set) with path compression and union by rank
# ---------------------------------------------------------------------------

class UnionFind:
    """Pickleable Union-Find data structure for connected components."""

    def __init__(self):
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}
        self.size: Dict[str, int] = {}

    def add(self, x: str) -> None:
        """Ensure node *x* exists in the structure."""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            self.size[x] = 1

    def find(self, x: str) -> str:
        """Find the root of *x* with path compression."""
        # Iterative path compression (fully picklable, no recursion)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression
        while self.parent[x] != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, a: str, b: str) -> Tuple[bool, int]:
        """Merge the sets containing *a* and *b*.

        Returns ``(merged, new_size)`` where *merged* is ``True`` if a
        merge actually happened (the nodes were in different sets) and
        *new_size* is the size of the resulting component.
        """
        ra = self.find(a)
        rb = self.find(b)

        if ra == rb:
            return False, self.size[ra]

        # Union by rank
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra

        self.parent[rb] = ra
        self.size[ra] += self.size[rb]

        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

        return True, self.size[ra]

    def component_size(self, x: str) -> int:
        """Return the size of the component containing *x*."""
        return self.size[self.find(x)]

    def __getstate__(self):
        return (self.parent, self.rank, self.size)

    def __setstate__(self, state):
        self.parent, self.rank, self.size = state


# ---------------------------------------------------------------------------
# Input source – reads edges from a JSONL file
# ---------------------------------------------------------------------------

class JSONLEdgeSource(FixedPartitionedSource):
    """A single-partition source that yields edges from a JSONL file."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state):
        return _JSONLEdgePartition(self._filepath, resume_state)


class _JSONLEdgePartition(StatefulSourcePartition):
    def __init__(self, filepath: str, resume_state=None):
        self._filepath = filepath
        self._idx = resume_state if resume_state is not None else 0
        self._lines = None

    def _load_lines(self):
        if self._lines is None:
            with open(self._filepath, "r") as fh:
                self._lines = fh.readlines()

    def next_batch(self):
        self._load_lines()
        if self._idx >= len(self._lines):
            raise StopIteration
        line = self._lines[self._idx]
        self._idx += 1
        obj = json.loads(line)
        return [(obj["u"], obj["v"])]

    def snapshot(self):
        return self._idx


# ---------------------------------------------------------------------------
# Output sink – writes JSON objects to a JSONL file
# ---------------------------------------------------------------------------

class JSONLSink(FixedPartitionedSink):
    """A single-partition sink that writes JSON lines to a file."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state):
        return _JSONLSinkPartition(self._filepath)


class _JSONLSinkPartition(StatefulSinkPartition):
    def __init__(self, filepath: str):
        self._filepath = filepath
        self._fh = None

    def write_batch(self, items):
        if self._fh is None:
            self._fh = open(self._filepath, "w")
        for item in items:
            self._fh.write(json.dumps(item) + "\n")
            self._fh.flush()

    def close(self):
        if self._fh is not None:
            self._fh.close()

    def snapshot(self):
        return None


# ---------------------------------------------------------------------------
# Stateful mapper logic
# ---------------------------------------------------------------------------

def union_find_mapper(
    uf: Optional[UnionFind], edge: Tuple[str, str]
) -> Tuple[Optional[UnionFind], Dict[str, Any]]:
    """Stateful mapper: maintain UnionFind and emit an event per edge."""
    if uf is None:
        uf = UnionFind()

    u, v = edge
    uf.add(u)
    uf.add(v)

    merged, size = uf.union(u, v)

    if merged:
        return uf, {"u": u, "v": v, "status": "merged", "new_component_size": size}
    else:
        return uf, {"u": u, "v": v, "status": "already_connected", "component_size": size}


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

def build_flow() -> Dataflow:
    """Build and return the connected-components Bytewax dataflow."""

    run_id = os.environ.get("ZEALT_RUN_ID", "default")
    input_path = os.path.join(os.path.dirname(__file__), "input_edges.jsonl")
    output_path = os.path.join(
        os.path.dirname(__file__), f"output_events_{run_id}.jsonl"
    )

    flow = Dataflow("connected_components")

    # 1. Read edges from JSONL
    edges = op.input("read_edges", flow, JSONLEdgeSource(input_path))

    # 2. Route ALL edges to a single key so stateful_map sees the global view
    keyed = op.key_on("route_to_single_partition", edges, lambda _: "global")

    # 3. Stateful processing with Union-Find
    events = op.stateful_map("union_find", keyed, union_find_mapper)

    # 4. Write output events
    op.output("write_events", events, JSONLSink(output_path))

    return flow


# Module-level flow variable for the Bytewax CLI
flow = build_flow()
