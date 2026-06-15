import json
import os
import pathlib
from typing import Optional, Tuple

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink

class UnionFind:
    def __init__(self):
        # Maps node to its parent
        self.parent = {}
        # Maps representative node to its component size
        self.size = {}

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1

    def find(self, x):
        self.add(x)
        # Path compression
        curr = x
        while self.parent[curr] != curr:
            curr = self.parent[curr]
        
        # Compress path
        root = curr
        curr = x
        while curr != root:
            nxt = self.parent[curr]
            self.parent[curr] = root
            curr = nxt
        return root

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

def parse_json(line: str) -> dict:
    return json.loads(line)

def update_uf(uf: Optional[UnionFind], edge: dict) -> Tuple[UnionFind, dict]:
    if uf is None:
        uf = UnionFind()
    
    u = edge["u"]
    v = edge["v"]
    merged, size = uf.union(u, v)
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
    return uf, result

def format_output(keyed_item: Tuple[str, dict]) -> Tuple[str, str]:
    key, result = keyed_item
    return key, json.dumps(result)

# Build the dataflow
flow = Dataflow("graph_pipeline")

# Input
input_path = "/home/user/graph_pipeline/input_edges.jsonl"
input_stream = op.input("inp", flow, FileSource(input_path))

# Parse JSON
parsed_stream = op.map("parse_json", input_stream, parse_json)

# Key on a constant to route all to a single partition
keyed_stream = op.key_on("key_edges", parsed_stream, lambda _: "GLOBAL")

# Stateful Union-Find mapping
stateful_stream = op.stateful_map("union_find", keyed_stream, update_uf)

# Format to JSON string
formatted_stream = op.map("format_output", stateful_stream, format_output)

# Output
run_id = os.environ.get("ZEALT_RUN_ID") or "default"
output_path = f"/home/user/graph_pipeline/output_events_{run_id}.jsonl"
op.output("out", formatted_stream, FileSink(pathlib.Path(output_path)))
