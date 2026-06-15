import os
import json
import pytest

PROJECT_DIR = "/home/user/graph_pipeline"

def get_run_id():
    run_id = os.environ.get("ZEALT_RUN_ID")
    if not run_id:
        pytest.fail("ZEALT_RUN_ID environment variable is not set.")
    return run_id

def test_output_file_exists():
    run_id = get_run_id()
    output_file = os.path.join(PROJECT_DIR, f"output_events_{run_id}.jsonl")
    assert os.path.isfile(output_file), f"Output file {output_file} does not exist."

def test_output_file_contents():
    run_id = get_run_id()
    output_file = os.path.join(PROJECT_DIR, f"output_events_{run_id}.jsonl")
    
    with open(output_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 5, f"Expected 5 lines of output, but found {len(lines)}."
    
    try:
        events = [json.loads(line) for line in lines]
    except json.JSONDecodeError as exc:
        pytest.fail(f"Failed to parse output file as JSONL: {exc}")
        
    expected_sequence = [
        {"u": "A", "v": "B", "status": "merged", "new_component_size": 2},
        {"u": "B", "v": "C", "status": "merged", "new_component_size": 3},
        {"u": "A", "v": "C", "status": "already_connected", "component_size": 3},
        {"u": "D", "v": "E", "status": "merged", "new_component_size": 2},
        {"u": "C", "v": "E", "status": "merged", "new_component_size": 5}
    ]
    
    for i, (actual, expected) in enumerate(zip(events, expected_sequence)):
        assert actual == expected, (
            f"Mismatch at line {i+1}:\n"
            f"Expected: {expected}\n"
            f"Actual: {actual}"
        )
