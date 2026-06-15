import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import json
import os

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
        root = i
        while root != self.parent[root]:
            path.append(root)
            root = self.parent[root]
        for node in path:
            self.parent[node] = root
        return root

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

def process_edge(uf, edge_str):
    if uf is None:
        uf = UnionFind()

    edge = json.loads(edge_str)
    u = edge["u"]
    v = edge["v"]
    merged, size = uf.union(u, v)
    
    if merged:
        res = {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": size
        }
    else:
        res = {
            "u": u,
            "v": v,
            "status": "already_connected",
            "component_size": size
        }
    return uf, res

flow = Dataflow("connected_components")
inp = op.input("in", flow, FileSource("input_edges.jsonl"))
keyed = op.key_on("key", inp, lambda x: "global")
processed = op.stateful_map("union_find", keyed, process_edge)
res_stream = op.map("format_output", processed, lambda x: (x[0], json.dumps(x[1])))

run_id = os.environ.get("ZEALT_RUN_ID", "default")
out_file = f"output_events_{run_id}.jsonl"

# The file must exist
with open(out_file, "w") as f:
    pass

op.output("out", res_stream, FileSink(out_file))
