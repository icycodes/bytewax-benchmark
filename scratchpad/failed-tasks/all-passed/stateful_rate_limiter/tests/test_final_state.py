import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax-rate-limiter"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup the test environment before running tests."""
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    input_data = [
        {"user_id": "u1", "timestamp": 100.0, "cost": 5.0},
        {"user_id": "u1", "timestamp": 101.0, "cost": 6.0},
        {"user_id": "u2", "timestamp": 101.0, "cost": 11.0},
        {"user_id": "u1", "timestamp": 102.0, "cost": 2.0},
        {"user_id": "u1", "timestamp": 105.0, "cost": -1.0},
        {"user_id": "u1", "timestamp": 105.5, "cost": 10.0}
    ]
    
    with open(INPUT_FILE, "w") as f:
        for item in input_data:
            f.write(json.dumps(item) + "\n")
            
    # Run the user's script
    result = subprocess.run(
        ["python", "rate_limiter.py", "--input", "input.jsonl", "--output", "output.jsonl"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    # We assert here to fail fast if the script crashes
    assert result.returncode == 0, f"Script failed to run:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    
def test_output_file_exists():
    """Verify that the output file was created."""
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."

def test_rate_limiting_logic():
    """Verify the rate limiting logic output."""
    with open(OUTPUT_FILE, "r") as f:
        lines = f.read().strip().split("\n")
        
    assert len(lines) == 6, f"Expected 6 lines in output, got {len(lines)}"
    
    outputs = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            outputs.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"Line {i+1} is not valid JSON: {line}")
            
    expected_outputs = [
        {"user_id": "u1", "timestamp": 100.0, "cost": 5.0, "allowed": True},
        {"user_id": "u1", "timestamp": 101.0, "cost": 6.0, "allowed": True},
        {"user_id": "u2", "timestamp": 101.0, "cost": 11.0, "allowed": False},
        {"user_id": "u1", "timestamp": 102.0, "cost": 2.0, "allowed": True},
        {"user_id": "u1", "timestamp": 105.0, "cost": -1.0, "allowed": True},
        {"user_id": "u1", "timestamp": 105.5, "cost": 10.0, "allowed": True}
    ]
    
    u1_outputs = [o for o in outputs if o.get("user_id") == "u1"]
    u2_outputs = [o for o in outputs if o.get("user_id") == "u2"]
    
    expected_u1 = [o for o in expected_outputs if o["user_id"] == "u1"]
    expected_u2 = [o for o in expected_outputs if o["user_id"] == "u2"]
    
    assert len(u1_outputs) == len(expected_u1), f"Mismatch in number of events for u1. Expected {len(expected_u1)}, got {len(u1_outputs)}"
    assert len(u2_outputs) == len(expected_u2), f"Mismatch in number of events for u2. Expected {len(expected_u2)}, got {len(u2_outputs)}"
    
    for i, (actual, expected) in enumerate(zip(u1_outputs, expected_u1)):
        assert actual.get("allowed") == expected["allowed"], \
            f"u1 event {i+1} allowed mismatch. Expected {expected['allowed']}, got {actual.get('allowed')}. Event: {actual}"
            
    for i, (actual, expected) in enumerate(zip(u2_outputs, expected_u2)):
        assert actual.get("allowed") == expected["allowed"], \
            f"u2 event {i+1} allowed mismatch. Expected {expected['allowed']}, got {actual.get('allowed')}. Event: {actual}"
