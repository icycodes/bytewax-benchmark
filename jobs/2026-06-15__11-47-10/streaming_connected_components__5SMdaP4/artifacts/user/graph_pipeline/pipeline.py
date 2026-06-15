import os
import json
from pathlib import Path
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.size = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1
            return x
        
        # Path compression
        path = []
        curr = x
        while self.parent[curr] != curr:
            path.append(curr)
            curr = self.parent[curr]
        for node in path:
            self.parent[node] = curr
        return curr

    def union(self, x, y):
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return False, self.size[root_x]

        # Union by size
        if self.size[root_x] < self.size[root_y]:
            root_x, root_y = root_y, root_x

        self.parent[root_y] = root_x
        self.size[root_x] += self.size[root_y]
        return True, self.size[root_x]

def process_edge(state, edge):
    if state is None:
        state = UnionFind()
    
    u = edge["u"]
    v = edge["v"]
    
    merged, size = state.union(u, v)
    if merged:
        result = {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": size
        }
    else:
        result = {
            "u": u,
            "v": v,
            "status": "already_connected",
            "component_size": size
        }
    
    return state, result

# Construct paths
run_id = os.environ.get("ZEALT_RUN_ID", "test")
input_path = Path("/home/user/graph_pipeline/input_edges.jsonl")
output_path = Path(f"/home/user/graph_pipeline/output_events_{run_id}.jsonl")

flow = Dataflow("connected_components")

# 1. Read input edges line-by-line
edges_raw = op.input("input_edges", flow, FileSource(input_path))

# 2. Parse JSON lines
edges_parsed = op.map("parse_json", edges_raw, json.loads)

# 3. Route all edges to a single partition using a constant key
edges_keyed = op.key_on("key_edges", edges_parsed, lambda edge: "global_key")

# 4. Maintain the connected components statefully
components_keyed = op.stateful_map("union_find", edges_keyed, process_edge)

# 5. Extract results and format as JSON lines (as a keyed stream for FileSink)
output_lines = op.map("format_output", components_keyed, lambda x: (x[0], json.dumps(x[1])))

# 6. Write output to file sink
op.output("output_events", output_lines, FileSink(output_path))
