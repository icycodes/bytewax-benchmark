import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/myproject"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup the test environment by creating the input file and removing the output file."""
    os.makedirs(PROJECT_DIR, exist_ok=True)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    input_data = [
        {"time": "2023-01-01T12:05:00Z", "word": "apple"},
        {"time": "2023-01-01T12:15:00Z", "word": "apple"},
        {"time": "2023-01-01T12:30:00Z", "word": "banana"},
        {"time": "2023-01-01T13:05:00Z", "word": "apple"},
        {"time": "2023-01-01T13:10:00Z", "word": "cherry"}
    ]
    
    with open(INPUT_FILE, "w") as f:
        for item in input_data:
            f.write(json.dumps(item) + "\n")

def test_run_dataflow():
    """Run the dataflow and verify it completes successfully."""
    result = subprocess.run(
        ["python", "dataflow.py", "input.jsonl", "output.jsonl"],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Dataflow execution failed: {result.stderr}"
    assert os.path.isfile(OUTPUT_FILE), "output.jsonl was not created."

def test_verify_output():
    """Verify the contents of the output file match the expected results."""
    assert os.path.isfile(OUTPUT_FILE), "output.jsonl does not exist."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = f.readlines()
        
    results = []
    for line in lines:
        try:
            results.append(json.loads(line.strip()))
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON in output file: {line}")
            
    expected_results = [
        {"word": "apple", "window_id": "2023-01-01 12:00:00+00:00", "count": 2},
        {"word": "banana", "window_id": "2023-01-01 12:00:00+00:00", "count": 1},
        {"word": "apple", "window_id": "2023-01-01 13:00:00+00:00", "count": 1},
        {"word": "cherry", "window_id": "2023-01-01 13:00:00+00:00", "count": 1}
    ]
    
    assert len(results) == len(expected_results), f"Expected {len(expected_results)} output lines, got {len(results)}"
    
    for expected in expected_results:
        assert expected in results, f"Expected output {expected} not found in {results}"
