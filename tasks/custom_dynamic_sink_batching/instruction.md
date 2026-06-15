# Custom Dynamic Sink with File Rotation

## Background
Bytewax provides `DynamicSink` and `StatelessSinkPartition` for implementing custom output destinations. In this task, you will implement a highly complex custom sink that handles file rotation and exact record limits per file, properly handling variable-sized batches provided by the Bytewax runtime.

## Requirements
- Create a Bytewax dataflow in `run.py` that processes a stream of 200 integers (0 to 199).
- Implement a custom `DynamicSink` that writes these integers to JSON Lines (`.jsonl`) files.
- The dataflow will be executed with 4 workers. Each worker must write to its own isolated files to avoid concurrent write conflicts.
- The custom sink must implement strict file rotation: a single file must contain **exactly 20 records** (except possibly the last file for a worker, which may have fewer). 
- Once a file has 20 records written to it, it must be closed, and subsequent records for that worker must be written to the next part number.
- The records written to the JSONL file must be JSON objects with the format `{"worker": <worker_index>, "value": <value>}`.
- You must read the `run-id` from the `ZEALT_RUN_ID` environment variable.
- The output files must be saved in the `out/` directory and named using the format: `output-<run-id>-worker-<worker_index>-part-<part_number>.jsonl` (starting with part 0).

## Implementation Hints
- Subclass `StatelessSinkPartition` to handle the `write_batch` logic. Keep track of the current part number and the number of records written to the current file.
- Subclass `DynamicSink` to instantiate the partition for each worker, passing the `worker_index` to the partition.
- Use `bytewax.testing.TestingSource` to generate the 200 integers.
- **Crucial**: The `write_batch` method receives a list of items of arbitrary size. You must handle the case where a single batch spans across multiple files. For example, if the batch has 25 items and the current file already has 15 items, you must write 5 items to the current file, close it, open the next part, write 20 items, close it, and open the next part to write the remaining 0 items (or wait until the next batch to write more).

## Acceptance Criteria
- Project path: `/home/user/bytewax-sink`
- Command: `python3 -m bytewax.run run:flow -w 4`
- The output files must be created in the `/home/user/bytewax-sink/out/` directory.
- There must be exactly 200 records written in total across all files.
- No single file can have more than 20 lines.
- The files must be named `output-<run-id>-worker-<worker_index>-part-<part_number>.jsonl` where `run-id` is read from the `ZEALT_RUN_ID` environment variable.

