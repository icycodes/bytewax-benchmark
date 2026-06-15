import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery")

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup input file and recovery directory before testing."""
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    os.makedirs(RECOVERY_DIR, exist_ok=True)
    subprocess.run(["python", "-m", "bytewax.recovery", RECOVERY_DIR, "1"], check=True)
    
    input_data = [
        {"user_id": "u1", "event_id": "e1", "timestamp": 100.0},
        {"user_id": "u1", "event_id": "e2", "timestamp": 100.1},
        {"user_id": "u1", "event_id": "e3", "timestamp": 100.2},
        {"user_id": "u1", "event_id": "e4", "timestamp": 100.3},
        {"user_id": "u1", "event_id": "e5", "timestamp": 100.4},
        {"user_id": "u1", "event_id": "e6", "timestamp": 100.5},
        {"user_id": "u1", "event_id": "e7", "timestamp": 102.5},
        {"user_id": "u2", "event_id": "e8", "timestamp": 100.0}
    ]
    with open(INPUT_FILE, "w") as f:
        for event in input_data:
            f.write(json.dumps(event) + "\n")

def test_pipeline_execution():
    """Run the pipeline with recovery enabled and verify it succeeds."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "dataflow:flow", "-r", "./recovery", "-b", "0"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline execution failed: {result.stderr}"

def test_rate_limiting_output():
    """Verify the output JSON lines match the expected rate-limited events."""
    assert os.path.exists(OUTPUT_FILE), "Output file was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    
    assert len(lines) == 7, f"Expected exactly 7 allowed events, but got {len(lines)}."
    
    parsed_events = [json.loads(line) for line in lines]
    event_ids = [event["event_id"] for event in parsed_events]
    
    expected_event_ids = ["e1", "e2", "e3", "e4", "e5", "e7", "e8"]
    
    # Check that exactly the expected events are present (order might not be strictly guaranteed by Bytewax across partitions, but usually is for single partition)
    # So we use a set for comparison or sort them. But single partition should preserve order.
    # Let's just check the set to be safe.
    assert set(event_ids) == set(expected_event_ids), f"Expected events {expected_event_ids}, got {event_ids}"
    assert "e6" not in event_ids, "Event e6 should have been dropped due to rate limiting."