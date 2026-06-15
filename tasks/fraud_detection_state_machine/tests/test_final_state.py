import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/fraud_detection"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")
RUN_FILE = os.path.join(PROJECT_DIR, "run.py")

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup test data before running tests."""
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    test_data = [
        {"user_id": "u1", "event_type": "login", "timestamp": 1000},
        {"user_id": "u1", "event_type": "transaction", "amount": 1500, "timestamp": 1050},
        {"user_id": "u1", "event_type": "transaction", "amount": 1200, "timestamp": 1100},
        {"user_id": "u1", "event_type": "transaction", "amount": 2000, "timestamp": 1200},
        {"user_id": "u2", "event_type": "login", "timestamp": 1000},
        {"user_id": "u2", "event_type": "transaction", "amount": 1500, "timestamp": 1100},
        {"user_id": "u2", "event_type": "logout", "timestamp": 1150},
        {"user_id": "u2", "event_type": "transaction", "amount": 1500, "timestamp": 1200},
        {"user_id": "u2", "event_type": "transaction", "amount": 1500, "timestamp": 1250},
        {"user_id": "u3", "event_type": "login", "timestamp": 1000},
        {"user_id": "u3", "event_type": "transaction", "amount": 1500, "timestamp": 1100},
        {"user_id": "u3", "event_type": "transaction", "amount": 1500, "timestamp": 1200},
        {"user_id": "u3", "event_type": "transaction", "amount": 1500, "timestamp": 1350}
    ]
    
    with open(INPUT_FILE, "w") as f:
        for item in test_data:
            f.write(json.dumps(item) + "\n")
    
    yield
    
    # Cleanup
    if os.path.exists(INPUT_FILE):
        os.remove(INPUT_FILE)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

def test_run_script_executes_successfully():
    """Verify that the command completes successfully with exit code 0."""
    result = subprocess.run(
        ["python", "run.py", "--input", "input.jsonl", "--output", "output.jsonl"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"run.py failed with error: {result.stderr}"

def test_output_contains_correct_fraud_alerts():
    """Verify that output.jsonl contains exactly one line with the correct alert."""
    assert os.path.exists(OUTPUT_FILE), "output.jsonl was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 1, f"Expected exactly 1 alert, found {len(lines)}."
    
    try:
        alert = json.loads(lines[0])
    except json.JSONDecodeError:
        pytest.fail(f"Output line is not valid JSON: {lines[0]}")
        
    assert alert.get("user_id") == "u1", f"Expected alert for user 'u1', got {alert.get('user_id')}"
    assert alert.get("alert") == "FRAUD_ALERT", f"Expected alert 'FRAUD_ALERT', got {alert.get('alert')}"

def test_bytewax_usage():
    """Inspect run.py to ensure bytewax is imported and a stateful operator is used."""
    assert os.path.exists(RUN_FILE), "run.py does not exist."
    
    with open(RUN_FILE, "r") as f:
        content = f.read()
        
    assert "bytewax" in content, "Bytewax is not imported in run.py."
    assert "stateful" in content or "fold_window" in content or "reduce_window" in content, \
        "No stateful operator (e.g., stateful_map, stateful_batch) found in run.py."
