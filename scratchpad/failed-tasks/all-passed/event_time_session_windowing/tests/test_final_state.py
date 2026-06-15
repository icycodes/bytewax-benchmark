import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

def test_pipeline_execution():
    """Create input data, run the pipeline, and verify the output."""
    # Setup test data
    input_data = [
        {"user_id": "u1", "page": "/home", "timestamp": "2026-01-01T12:00:00Z"},
        {"user_id": "u1", "page": "/about", "timestamp": "2026-01-01T12:00:05Z"},
        {"user_id": "u2", "page": "/contact", "timestamp": "2026-01-01T12:00:00Z"},
        {"user_id": "u1", "page": "/pricing", "timestamp": "2026-01-01T12:00:20Z"},
        {"user_id": "u2", "page": "/home", "timestamp": "2026-01-01T12:00:08Z"}
    ]
    
    with open(INPUT_FILE, "w") as f:
        for record in input_data:
            f.write(json.dumps(record) + "\n")
            
    # Ensure output file is removed before running
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    # Run the pipeline
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0, f"Pipeline execution failed with exit code {result.returncode}.\nStderr: {result.stderr}\nStdout: {result.stdout}"
    
    # Verify the output
    assert os.path.exists(OUTPUT_FILE), "output.jsonl was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 3, f"Expected exactly 3 lines in output.jsonl, found {len(lines)}."
    
    parsed_results = []
    for line in lines:
        try:
            parsed_results.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"Could not parse line as JSON: {line}")
            
    # Expected results
    expected_u1_session1 = {"user_id": "u1", "total_pages": 2}
    expected_u1_session2 = {"user_id": "u1", "total_pages": 1}
    expected_u2_session = {"user_id": "u2", "total_pages": 2}
    
    assert expected_u1_session1 in parsed_results, f"Missing expected result {expected_u1_session1} in {parsed_results}"
    assert expected_u1_session2 in parsed_results, f"Missing expected result {expected_u1_session2} in {parsed_results}"
    assert expected_u2_session in parsed_results, f"Missing expected result {expected_u2_session} in {parsed_results}"
