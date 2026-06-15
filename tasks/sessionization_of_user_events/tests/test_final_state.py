import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax-sessionization"

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Ensure sessions.jsonl is removed before tests run."""
    output_file = os.path.join(PROJECT_DIR, "sessions.jsonl")
    if os.path.isfile(output_file):
        os.remove(output_file)
    yield

def test_dataflow_execution():
    """Run the dataflow script and verify it completes successfully."""
    result = subprocess.run(
        ["python", "run.py"],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Dataflow execution failed: {result.stderr}"

def test_output_file_exists():
    """Check that the output file sessions.jsonl was created."""
    output_file = os.path.join(PROJECT_DIR, "sessions.jsonl")
    assert os.path.isfile(output_file), f"Output file {output_file} was not created."

def test_session_metrics():
    """Verify the exact contents of the output file against expected sessionization logic."""
    output_file = os.path.join(PROJECT_DIR, "sessions.jsonl")
    
    sessions = []
    with open(output_file, "r") as f:
        for line in f:
            if line.strip():
                sessions.append(json.loads(line))
    
    assert len(sessions) > 0, "The output file sessions.jsonl is empty."
    
    # Sort sessions by user_id and then session_start to make assertions deterministic
    sessions.sort(key=lambda x: (x["user_id"], x["session_start"]))
    
    # We expect specific sessions based on the user_events.jsonl generated in the Dockerfile
    expected_sessions = [
        {
            "user_id": "user1",
            "session_start": "2023-10-01T10:00:00Z",
            "session_end": "2023-10-01T10:15:00Z",
            "event_count": 2
        },
        {
            "user_id": "user1",
            "session_start": "2023-10-01T11:00:00Z",
            "session_end": "2023-10-01T11:00:00Z",
            "event_count": 1
        },
        {
            "user_id": "user2",
            "session_start": "2023-10-01T10:05:00Z",
            "session_end": "2023-10-01T10:30:00Z",
            "event_count": 3
        }
    ]
    
    assert len(sessions) == len(expected_sessions), f"Expected {len(expected_sessions)} sessions, got {len(sessions)}"
    
    for i, expected in enumerate(expected_sessions):
        actual = sessions[i]
        assert actual["user_id"] == expected["user_id"], f"Expected user_id {expected['user_id']}, got {actual['user_id']}"
        assert actual["session_start"] == expected["session_start"], f"Expected session_start {expected['session_start']}, got {actual['session_start']}"
        assert actual["session_end"] == expected["session_end"], f"Expected session_end {expected['session_end']}, got {actual['session_end']}"
        assert actual["event_count"] == expected["event_count"], f"Expected event_count {expected['event_count']}, got {actual['event_count']}"
