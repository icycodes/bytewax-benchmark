# Real-Time ML Inference Pipeline

## Background
Build a real-time text embedding pipeline using Bytewax and `sentence-transformers`.

## Requirements
- Implement a Bytewax dataflow in `pipeline.py` that reads a JSONL file of text documents.
- Generate vector embeddings for the text using the `all-MiniLM-L6-v2` model.
- Write the results to a JSONL file.
- The pipeline MUST support fault tolerance by enabling SQLite recovery. Ensure that the unpicklable ML model does not cause pickling errors during state snapshots.

## Implementation Hints
- Read the `run-id` from the `ZEALT_RUN_ID` environment variable to name your output file.
- To avoid `PicklingError` during recovery, initialize your ML model lazily in a stateless operator or worker context rather than storing it in a stateful operator's state.

## Acceptance Criteria
- Project path: /home/user/ml_pipeline
- Command: `python -m bytewax.run pipeline:flow -r ./recovery_dir -s 1`
- The pipeline reads from `input.jsonl` in the project directory. Each line is a JSON object: `{"doc_id": "...", "text": "..."}`.
- The pipeline writes the embeddings to `embeddings-${run-id}.jsonl` in the project directory.
- Each line in the output must be a JSON object: `{"doc_id": "...", "embedding": [...]}` where `embedding` is a list of floats representing the `all-MiniLM-L6-v2` vector.
- The pipeline must execute successfully with SQLite recovery enabled (`-r ./recovery_dir`) without throwing any pickling errors.
