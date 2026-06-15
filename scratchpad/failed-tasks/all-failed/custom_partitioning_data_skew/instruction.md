# Bytewax Custom Partitioning Strategy

## Background
You are building a Bytewax dataflow to process a large dataset from a local file. To handle potential data skew and ensure the workload is evenly distributed across workers, you need to implement a custom partitioning strategy at the source level. Instead of reading the entire file in a single partition, you will implement a custom partitioned source that distributes the data across multiple partitions.

## Requirements
- Write a Bytewax dataflow script in `dataflow.py`.
- Implement a custom input source by subclassing Bytewax's `FixedPartitionedSource` (and `StatefulSourcePartition` or equivalent based on the Bytewax version).
- The custom source must read from `input.jsonl`.
- The custom source must define exactly 3 partitions (e.g., `"0"`, `"1"`, `"2"`).
- Implement a round-robin partitioning strategy: line 0 goes to partition `"0"`, line 1 to partition `"1"`, line 2 to partition `"2"`, line 3 to partition `"0"`, and so on.
- The dataflow should process these lines and write them to `output.jsonl` using a standard Bytewax output sink.
- The output lines should match the input lines.

## Implementation Hints
- Check the Bytewax documentation for the exact API of `FixedPartitionedSource` and `StatefulSourcePartition` for the installed version.
- In your partition builder, you can read the file and skip lines that do not belong to the current partition (using the line index modulo 3).
- Ensure your stateful partition implementation correctly yields batches of data and handles the end of the file.

## Acceptance Criteria
- Project path: /home/user/myproject
- Command: python -m bytewax.run dataflow:flow
- The command must execute successfully and create `output.jsonl`.
- The output file must contain all the lines from `input.jsonl`.
- The custom source must distribute the workload across exactly 3 partitions using round-robin logic.
