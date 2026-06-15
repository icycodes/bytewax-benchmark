# Streaming Connected Components with Bytewax

## Background
You are building a real-time graph analytics pipeline using Bytewax (v0.21.1). The pipeline processes a stream of new connections (edges) between nodes and dynamically maintains the connected components (communities) of the graph.

## Requirements
- Create a Bytewax dataflow in `pipeline.py`.
- **Input**: Read edges from `input_edges.jsonl`. Each line is a JSON object: `{"u": "node1", "v": "node2"}`.
- **Stateful Processing**: Maintain a Union-Find (Disjoint Set) data structure to track connected components. Since Union-Find requires a global view, ensure all edges are routed to a single stateful partition.
- **Output Logic**: For each edge `(u, v)`:
  - If `u` and `v` are already in the same component, emit: `{"u": u, "v": v, "status": "already_connected", "component_size": <size of the component>}`.
  - If `u` and `v` are in different components, merge them and emit: `{"u": u, "v": v, "status": "merged", "new_component_size": <size of the new merged component>}`.
- **Sink**: Write the emitted JSON objects to `output_events_${run-id}.jsonl`.
- **Execution**: The pipeline must be executable via the Bytewax CLI with SQLite recovery enabled.

## Implementation Hints
- Read the `ZEALT_RUN_ID` environment variable to construct the output filename.
- Use a constant key to route all edges to a single `stateful_map` partition.
- Ensure your Union-Find state is fully picklable to support Bytewax's SQLite recovery.

## Acceptance Criteria
- Project path: `/home/user/graph_pipeline`
- Ensure the script is executed and the artifacts exist.
- Output artifact: `/home/user/graph_pipeline/output_events_${run-id}.jsonl`
- The output file must contain exactly one JSON object per line, corresponding to the input edges in order.
- The output must correctly reflect the Union-Find logic, accurately reporting `merged` or `already_connected` and the correct component sizes.

