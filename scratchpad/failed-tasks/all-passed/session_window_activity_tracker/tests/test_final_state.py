import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/project"
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

def test_run_dataflow():
    """Run the Bytewax dataflow and verify it succeeds."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Dataflow execution failed with error:\n{result.stderr}\nStdout:\n{result.stdout}"

def test_output_file_exists():
    """Verify the output file was created."""
    assert os.path.isfile(OUTPUT_FILE), f"Expected output file {OUTPUT_FILE} was not created."

def test_output_file_contents():
    """Verify the output file contains the correct sessions."""
    with open(OUTPUT_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    
    sessions = []
    for line in lines:
        try:
            data = json.loads(line)
            sessions.append(data)
        except json.JSONDecodeError:
            pytest.fail(f"Output line is not valid JSON: {line}")
            
    # Check for specific sessions
    u1_session_1 = {"user_id": "u1", "events": ["login", "view_item"]}
    u1_session_2 = {"user_id": "u1", "events": ["add_to_cart"]}
    u2_session_1 = {"user_id": "u2", "events": ["login", "checkout"]}
    
    assert u1_session_1 in sessions, f"Expected session {u1_session_1} not found in output. Output: {sessions}"
    assert u1_session_2 in sessions, f"Expected session {u1_session_2} not found in output. Output: {sessions}"
    assert u2_session_1 in sessions, f"Expected session {u2_session_1} not found in output. Output: {sessions}"
    
    assert len(sessions) == 3, f"Expected exactly 3 sessions, found {len(sessions)}. Output: {sessions}"
