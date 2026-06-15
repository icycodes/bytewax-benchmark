# Bytewax Stream Join with Type Casting and Recovery

## Background
In ad-tech, it is common to join two streams of data: ad impressions and ad clicks. You need to build a Bytewax dataflow that processes these two streams, joins them on the ad ID, and outputs the joined records. The dataflow must be fully compatible with Bytewax's SQLite recovery mechanism.

## Requirements
- Read impressions from `impressions.jsonl`. Each line is a JSON object with `ad_id` (an integer) and `user_id` (a string).
- Read clicks from `clicks.jsonl`. Each line is a JSON object with `ad_id` (a string) and `click_time` (a string).
- Parse the JSON lines into Python dictionaries.
- Join the two streams on `ad_id`. Note the type mismatch: `ad_id` is an integer in the impressions stream but a string in the clicks stream.
- Format the joined result into a JSON string containing the keys `ad_id`, `user_id`, and `click_time`.
- Output the joined JSON strings to `joined.jsonl`.
- The pipeline must be runnable with SQLite recovery enabled, meaning all state objects must be picklable.

## Implementation Hints
- Use `bytewax.connectors.files.FileSource` to read the JSONL files and `FileSink` to write the output.
- Use `bytewax.operators.map` to parse JSON strings and to format the output.
- Use `bytewax.operators.key_on` to convert the streams into Keyed Streams. Bytewax strictly requires routing keys to be Python strings.
- Use `bytewax.operators.join` to perform an inner join on the keyed streams.
- Ensure your pipeline logic does not store unpicklable objects (like lambdas, file handles, or generators) in state, as this will crash the pipeline when recovery snapshots are taken.

## Acceptance Criteria
- Project path: /home/user/ad_pipeline
- Command: python -m bytewax.run pipeline:flow -r ./recovery_dir
- The input files `impressions.jsonl` and `clicks.jsonl` will be provided in the project directory.
- The output file `joined.jsonl` must be created in the project directory.
- The output file `joined.jsonl` must contain one JSON object per line, with keys `ad_id`, `user_id`, and `click_time`.
- The command must exit with status code 0.

