# Bytewax Stateful Pipeline with Recovery

## Background
Bytewax is a stateful stream processing framework. In this task, you will build a Bytewax dataflow that processes an input stream of key-value pairs, maintains a stateful running maximum for each key, and persists its state using Bytewax's recovery mechanisms.

## Requirements
- Create a Bytewax dataflow that reads an input CSV file containing `key,value` pairs.
- Compute the running maximum of the `value` for each `key` using a stateful operator.
- Write the results to an output file in the format `key,running_max`.
- Enable Bytewax's local SQLite-based recovery system so that the pipeline's state is persisted to a specified recovery directory.
- Ensure that if the pipeline is run sequentially multiple times with the same recovery directory but different input files, the state (running maximums) is correctly recovered and maintained across runs.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` and `FileSink` (or similar standard Bytewax connectors) for file I/O.
- Use a stateful operator like `stateful_map` to maintain the maximum value seen so far for each key.
- Remember that stateful operators require a unique `step_id` for recovery to work properly.
- Your entrypoint should be a shell script `run.sh` that takes the input file, output file, and recovery directory as arguments, and invokes the Bytewax dataflow with the appropriate recovery flags (e.g., using `python -m bytewax.run` or equivalent CLI commands with `-r`, `-s`, `-b` flags as needed).

## Acceptance Criteria
- Project path: `/home/user/bytewax_recovery`
- Command: `bash run.sh <input_file> <output_file> <recovery_dir>`
- The script must execute the Bytewax dataflow with recovery enabled, storing SQLite recovery data in `<recovery_dir>`.
- The dataflow must read `key,value` pairs from `<input_file>` (where `value` is an integer).
- The dataflow must write the updated `key,running_max` to `<output_file>` for each processed event.
- The pipeline MUST support state recovery. When run sequentially with the same `<recovery_dir>`, the running maximums must not reset to zero but continue from the state saved in the previous run.

