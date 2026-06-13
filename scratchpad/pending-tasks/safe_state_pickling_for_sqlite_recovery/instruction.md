When fault-tolerance (recovery) is enabled, Bytewax uses Python's native `pickle` to serialize state objects to SQLite. Any unpicklable objects in the state will cause the pipeline to crash during snapshotting.

You need to create a stateful dataflow that calculates a running average of sensor temperatures. Define the state as a pure, serializable Python dictionary or dataclass. Ensure the pipeline logic isolates any unpicklable objects (like mock database connections or lambda functions) so they are never returned as part of the state in `op.stateful_map`.

**Constraints:**
- The returned state MUST be a pure Python dictionary, list, or dataclass.
- Do NOT store or retain open file handles, database connections, or lambda functions in the state object.
- The logic must be fully compatible with running via `python -m bytewax.run my_pipeline:flow -r ./recovery_dir`.