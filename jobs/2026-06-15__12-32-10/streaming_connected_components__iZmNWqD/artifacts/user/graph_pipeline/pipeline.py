import json
import os
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink


class UnionFind:
    """Picklable Union-Find (Disjoint Set) data structure.

    Maintains connected components with path compression and union by size.
    """

    def __init__(self):
        self.parent = {}
        self.size = {}

    def find(self, x):
        """Find the root of x with path compression."""
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        """Union the sets containing x and y.

        Returns:
            (merged, size): merged is True if x and y were in different sets,
            False if already in the same set. size is the size of the
            resulting component.
        """
        rx = self.find(x)
        ry = self.find(y)
        if rx == ry:
            return False, self.size[rx]
        # Union by size: attach smaller tree under larger tree
        if self.size[rx] < self.size[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        self.size[rx] += self.size[ry]
        return True, self.size[rx]


def uf_mapper(state, edge):
    """Stateful mapper that processes edges through Union-Find.

    For each edge (u, v):
    - If u and v are already in the same component, emit already_connected.
    - If u and v are in different components, merge them and emit merged.
    """
    if state is None:
        state = UnionFind()
    u = edge["u"]
    v = edge["v"]
    # Ensure both nodes exist in the structure
    state.find(u)
    state.find(v)
    merged, size = state.union(u, v)
    if merged:
        result = {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": size,
        }
    else:
        result = {
            "u": u,
            "v": v,
            "status": "already_connected",
            "component_size": size,
        }
    return (state, result)


run_id = os.environ.get("ZEALT_RUN_ID", "default")
input_path = "/home/user/graph_pipeline/input_edges.jsonl"
output_path = f"/home/user/graph_pipeline/output_events_{run_id}.jsonl"

flow = Dataflow("graph_pipeline")

# Read edges from JSONL file
inp = op.input("read_edges", flow, FileSource(input_path))

# Parse each line from JSON string to dict
parsed = op.map("parse_json", inp, json.loads)

# Route all edges to a single partition using a constant key
# This ensures the Union-Find state is maintained globally
keyed = op.key_on("constant_key", parsed, lambda _: "ALL")

# Stateful processing: update Union-Find and emit results
result = op.stateful_map("union_find", keyed, uf_mapper)

# Format the value as a JSON string (keep the key for FileSink routing)
formatted = op.map_value("format_output", result, json.dumps)

# Write output to JSONL file
op.output("write_output", formatted, FileSink(output_path))