import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax_routing"
INPUT_FILE = "test_input.jsonl"

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup test input file and clean up existing output files."""
    os.chdir(PROJECT_DIR)
    
    # Create test input file
    test_data = [
        '{"id": 1, "type": "log", "message": "System started"}\n',
        '{"id": 2, "type": "error", "code": 500}\n',
        '{"id": 3, "type": "metric", "value": 42.5}\n',
        '{"id": 4, "message": "Missing type field"}\n',
        '{"id": 5, "type": "unknown_type", "data": "foo"}\n',
        '{"id": 6, "type": "log", "message": "User logged in"}\n'
    ]
    with open(INPUT_FILE, "w") as f:
        f.writelines(test_data)
        
    # Remove existing output files
    for filename in ["errors.jsonl", "metrics.jsonl", "logs.jsonl", "dead_letter.jsonl"]:
        if os.path.exists(filename):
            os.remove(filename)

def test_run_dataflow():
    """Run the Bytewax dataflow script."""
    result = subprocess.run(
        ["python", "run.py", "--input", INPUT_FILE],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Command failed with exit code {result.returncode}. stderr: {result.stderr}"

def test_logs_output():
    """Verify logs.jsonl contains the correct routed events."""
    filepath = os.path.join(PROJECT_DIR, "logs.jsonl")
    assert os.path.isfile(filepath), f"Output file {filepath} was not created."
    
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 2, f"Expected exactly 2 lines in logs.jsonl, found {len(lines)}"
    
    # Parse JSON to ignore formatting differences
    events = [json.loads(line) for line in lines]
    assert {"id": 1, "type": "log", "message": "System started"} in events, "Missing event id=1 in logs"
    assert {"id": 6, "type": "log", "message": "User logged in"} in events, "Missing event id=6 in logs"

def test_errors_output():
    """Verify errors.jsonl contains the correct routed events."""
    filepath = os.path.join(PROJECT_DIR, "errors.jsonl")
    assert os.path.isfile(filepath), f"Output file {filepath} was not created."
    
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 1, f"Expected exactly 1 line in errors.jsonl, found {len(lines)}"
    
    event = json.loads(lines[0])
    assert event == {"id": 2, "type": "error", "code": 500}, f"Unexpected event in errors: {event}"

def test_metrics_output():
    """Verify metrics.jsonl contains the correct routed events."""
    filepath = os.path.join(PROJECT_DIR, "metrics.jsonl")
    assert os.path.isfile(filepath), f"Output file {filepath} was not created."
    
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 1, f"Expected exactly 1 line in metrics.jsonl, found {len(lines)}"
    
    event = json.loads(lines[0])
    assert event == {"id": 3, "type": "metric", "value": 42.5}, f"Unexpected event in metrics: {event}"

def test_dead_letter_output():
    """Verify dead_letter.jsonl contains the correct routed events."""
    filepath = os.path.join(PROJECT_DIR, "dead_letter.jsonl")
    assert os.path.isfile(filepath), f"Output file {filepath} was not created."
    
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 2, f"Expected exactly 2 lines in dead_letter.jsonl, found {len(lines)}"
    
    events = [json.loads(line) for line in lines]
    assert {"id": 4, "message": "Missing type field"} in events, "Missing event id=4 in dead_letter"
    assert {"id": 5, "type": "unknown_type", "data": "foo"} in events, "Missing event id=5 in dead_letter"
