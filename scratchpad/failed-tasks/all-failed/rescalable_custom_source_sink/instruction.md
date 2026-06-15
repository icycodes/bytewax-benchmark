# Rescalable Custom Source and Sink

## Background
Bytewax provides `FixedPartitionedSource` and `FixedPartitionedSink` to build stateful, rescalable input and output connectors. These connectors maintain their position in the stream, allowing dataflows to recover gracefully from failures without data loss or duplication (exactly-once processing).

## Requirements
- Implement a `CustomFileSource` inheriting from `FixedPartitionedSource`. It should read all `.txt` files in a given `input_dir`. Each file acts as a distinct partition.
- The source must yield string items representing the lines of the files (without trailing newlines) and maintain the current line number as `resume_state` in its `StatefulSourcePartition`.
- Implement a `CustomFileSink` inheriting from `FixedPartitionedSink`. It should write items to a given `output_dir`. The partition key should be the filename, and the sink should append lines to the corresponding file.
- The sink's `StatefulSinkPartition` must maintain the number of lines written as `resume_state`. Upon initialization (or recovery), it must truncate the file to the exact number of lines specified in the `resume_state` to prevent duplicate writes.
- Create a dataflow in `pipeline.py` that reads from `CustomFileSource` (reading from `input_data/`), converts each line to uppercase using `op.map`, and writes to `CustomFileSink` (writing to `output_data/`).
- The dataflow must be executable via the Bytewax CLI with recovery enabled.

## Implementation Hints
- In `CustomFileSource.list_parts()`, return the list of filenames in the input directory.
- In `CustomFileSource.build_part()`, return a custom `StatefulSourcePartition` that opens the file, seeks to the correct line based on `resume_state`, and yields batches of lines in `next_batch()`.
- In `CustomFileSink.list_parts()`, return the same list of filenames.
- In `CustomFileSink.part_fn()`, route the item to the correct partition (you can format the output of the source as `(filename, line)` or just rely on a static mapping if the dataflow processes items one by one. Actually, for simplicity, have the source yield `(filename, line_content)`, use that key for the sink, and write the `line_content` to the file).
- Ensure both partitions return their current state in the `snapshot()` method.

## Acceptance Criteria
- Project path: /home/user/bytewax_project
- Ensure the script is executed and the artifacts exist.
- The source must read files from `/home/user/bytewax_project/input_data`.
- The sink must write files to `/home/user/bytewax_project/output_data`.
- The dataflow must process all lines, convert them to uppercase, and write them to the corresponding files in the output directory.
- The pipeline must support recovery. If interrupted and restarted with the same recovery directory, it should not produce duplicate lines in the output files.

