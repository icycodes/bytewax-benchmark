import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/app"

def test_pipeline_execution():
    """Run the Bytewax pipeline and verify the output."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Pipeline execution failed: {result.stderr}"
    
    output = result.stdout.strip()
    assert output, "Expected standard output from the pipeline, but got nothing."
    
    lines = output.split("\n")
    
    batches_user1 = 0
    batches_user2 = 0
    
    for line in lines:
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
            
        user_id = data.get("user_id")
        events = data.get("events", [])
        
        if user_id == "user1":
            batches_user1 += 1
            assert len(events) <= 3, f"user1 batch exceeded max_size of 3: {len(events)}"
        elif user_id == "user2":
            batches_user2 += 1
            assert len(events) <= 3, f"user2 batch exceeded max_size of 3: {len(events)}"
            
    assert batches_user1 == 2, f"Expected 2 batches for user1, got {batches_user1}"
    assert batches_user2 == 1, f"Expected 1 batch for user2, got {batches_user2}"
