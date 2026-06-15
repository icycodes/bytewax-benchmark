import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_merge_flow_output():
    """Verify that the Bytewax dataflow produces the correct joined output."""
    
    # Ensure the required files exist before running
    assert os.path.isfile(os.path.join(PROJECT_DIR, "users.jsonl")), "users.jsonl is missing"
    assert os.path.isfile(os.path.join(PROJECT_DIR, "emails.jsonl")), "emails.jsonl is missing"
    assert os.path.isfile(os.path.join(PROJECT_DIR, "activities.jsonl")), "activities.jsonl is missing"

    # Run the dataflow
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "merge_flow:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0, f"Bytewax dataflow failed to run. stderr: {result.stderr}"
    
    # Parse the output line by line as JSON
    output_lines = result.stdout.strip().split("\n")
    parsed_objects = []
    
    for line in output_lines:
        if not line.strip():
            continue
        try:
            parsed_objects.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"Output line is not a valid JSON string: {line}")
            
    # Define expected output objects
    expected_alice = {
        "id": "1",
        "name": "Alice",
        "email": "alice@example.com",
        "last_login": "2026-01-01"
    }
    expected_bob = {
        "id": "2",
        "name": "Bob",
        "email": "bob@example.com",
        "last_login": "2026-01-02"
    }
    
    # Verify exactly two objects are returned
    assert len(parsed_objects) == 2, f"Expected exactly 2 output objects, but got {len(parsed_objects)}: {parsed_objects}"
    
    # Verify the contents (order does not matter)
    assert expected_alice in parsed_objects, f"Expected Alice's profile in output, but not found. Output: {parsed_objects}"
    assert expected_bob in parsed_objects, f"Expected Bob's profile in output, but not found. Output: {parsed_objects}"
    
    # Check that user 3 and user 4 are not present
    for obj in parsed_objects:
        assert obj["id"] not in ["3", "4"], f"User {obj['id']} should not be in the output, as it is missing from some input streams."
