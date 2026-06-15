"""Bytewax pipeline for streaming connected components using Union-Find."""

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
import bytewax.operators as op

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent
INPUT_PATH = PROJECT_DIR / "input_edges.jsonl"
RUN_ID = os.environ.get("ZEALT_RUN_ID", "local")
OUTPUT_PATH = PROJECT_DIR / f"output_events_{RUN_ID}.jsonl"

# ---------------------------------------------------------------------------
# Union-Find (Disjoint Set) – fully picklable
# ---------------------------------------------------------------------------

class UnionFind:
    """Weighted quick-union with path compression."""

    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.size: Dict[str, int] = {}

    def _add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1

    def find(self, x: str) -> str:
        self._add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression
        while self.parent[x] != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, u: str, v: str) -> dict:
        """Process edge (u, v).  Return an event dict."""
        ru = self.find(u)
        rv = self.find(v)
        if ru == rv:
            return {
                "u": u,
                "v": v,
                "status": "already_connected",
                "component_size": self.size[ru],
            }
        # Weighted union – attach smaller tree under larger one
        if self.size[ru] < self.size[rv]:
            ru, rv = rv, ru  # ensure ru is the larger root
        self.parent[rv] = ru
        self.size[ru] += self.size[rv]
        return {
            "u": u,
            "v": v,
            "status": "merged",
            "new_component_size": self.size[ru],
        }


# ---------------------------------------------------------------------------
# Custom source – reads JSONL edges
# ---------------------------------------------------------------------------

class _JsonlSourcePartition(StatefulSourcePartition):
    """Read lines from a JSONL file, tracking position via resume_state."""

    def __init__(self, path: Path, resume_state: Optional[int]):
        self._path = path
        self._file = open(path, "r")
        if resume_state is not None:
            # Seek to the byte offset we left off at
            self._file.seek(resume_state)

    def next_batch(self) -> Iterable[dict]:
        batch: List[dict] = []
        for line in self._file:
            stripped = line.strip()
            if stripped:
                batch.append(json.loads(stripped))
        if not batch:
            raise StopIteration()
        return batch

    def next_awake(self) -> Optional:
        return None

    def snapshot(self) -> int:
        """Return current byte offset for recovery."""
        self._file.flush()
        return self._file.tell()

    def close(self) -> None:
        self._file.close()


class JsonlSource(FixedPartitionedSource):
    """Single-partition source that reads a JSONL file."""

    def __init__(self, path: Path):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _JsonlSourcePartition:
        return _JsonlSourcePartition(self._path, resume_state)


# ---------------------------------------------------------------------------
# Custom sink – writes JSONL events
# ---------------------------------------------------------------------------

class _JsonlSinkPartition(StatefulSinkPartition):
    """Append JSON objects as lines, tracking byte offset for recovery."""

    def __init__(self, path: Path, resume_state: Optional[int]):
        mode = "r+" if resume_state is not None else "w"
        self._path = path
        self._file = open(path, mode)
        if resume_state is not None:
            self._file.seek(resume_state)
        else:
            self._file.seek(0, 2)  # end of file for new writes

    def write_batch(self, items: List[dict]) -> None:
        for event in items:
            self._file.write(json.dumps(event) + "\n")
        self._file.flush()

    def snapshot(self) -> int:
        return self._file.tell()

    def close(self) -> None:
        self._file.close()


class JsonlSink(FixedPartitionedSink):
    """Single-partition sink that writes a JSONL file."""

    def __init__(self, path: Path):
        self._path = path

    def list_parts(self) -> List[str]:
        return ["single"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _JsonlSinkPartition:
        return _JsonlSinkPartition(self._path, resume_state)


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------

def _uf_mapper(
    state: Optional[UnionFind], edge: dict
) -> tuple[Optional[UnionFind], dict]:
    """Stateful mapper: maintain a Union-Find and emit an event per edge."""
    if state is None:
        state = UnionFind()
    result = state.union(edge["u"], edge["v"])
    return (state, result)


flow = Dataflow("connected_components")

# Read edges
inp = op.input("read_edges", flow, JsonlSource(INPUT_PATH))

# Route all edges to a single partition for global Union-Find state
keyed = op.key_on("constant_key", inp, lambda _edge: "uf")

# Stateful Union-Find processing
results = op.stateful_map("union_find", keyed, _uf_mapper)

# Write output (sink receives (key, event) tuples)
op.output("write_events", results, JsonlSink(OUTPUT_PATH))