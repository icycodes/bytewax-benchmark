Stateful operations in Bytewax use `op.stateful_map` to remember past events. However, modifying state in-place (e.g., directly appending to a list) causes silent state corruption during internal snapshotting.

You need to build a stateful dataflow using `op.stateful_map` that filters out duplicate message IDs from an incoming stream. The state should store a collection of seen IDs, but you must ensure the state is cloned or deep-copied rather than mutated in-place. Output only the unique messages.

**Constraints:**
- Do NOT use in-place mutations like `list.append()` or `set.add()` on the state object.
- The mapper function must return a newly created or deep-copied state structure along with the emitted value.
- Input must be a Keyed Stream before applying `stateful_map`.