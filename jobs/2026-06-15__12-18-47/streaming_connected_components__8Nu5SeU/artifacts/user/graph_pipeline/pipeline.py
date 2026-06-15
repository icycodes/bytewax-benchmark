import json
import os
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.size = {}

    def find(self, i):
        if i not in self.parent:
            self.parent[i] = i
            self.size[i] = 1
            return i
        
        path = []
        while self.parent[i] != i:
            path.append(i)
            i = self.parent[i]
            
        for node in path:
            self.parent[node] = i
            
        return i

    def union(self, i, j):
        root_i = self.find(i)
        root_j = self.find(j)
        
        if root_i == root_j:
            return False, self.size[root_i]
            
        if self.size[root_i] < self.size[root_j]:
            root_i, root_j = root_j, root_i
            
        self.parent[root_j] = root_i
        self.size[root_i] += self.size[root_j]
        
        return True, self.size[root_i]

class UFState:
    def __init__(self):
        self.uf = UnionFind()
        
    def process(self, edge):
        u = edge["u"]
        v = edge["v"]
        merged, size = self.uf.union(u, v)
        if merged:
            res = {"u": u, "v": v, "status": "merged", "new_component_size": size}
        else:
            res = {"u": u, "v": v, "status": "already_connected", "component_size": size}
        return self, res

def uf_mapper(state, edge):
    if state is None:
        state = UFState()
    return state.process(edge)

run_id = os.environ.get("ZEALT_RUN_ID", "default")
out_file = Path(f"output_events_{run_id}.jsonl")

flow = Dataflow("union_find")

# Read edges
inp = op.input("inp", flow, FileSource(Path("input_edges.jsonl")))

# Parse JSON
parsed = op.map("parse", inp, json.loads)

# Key to route all to a single stateful partition
keyed = op.key_on("key_on", parsed, lambda x: "GLOBAL")

# Stateful map to process union-find
processed = op.stateful_map("uf", keyed, uf_mapper)

# Format output as JSON string
def format_output(item):
    key, val = item
    return ("GLOBAL", json.dumps(val))

formatted = op.map("format", processed, format_output)

# Write to file
op.output("out", formatted, FileSink(out_file))
