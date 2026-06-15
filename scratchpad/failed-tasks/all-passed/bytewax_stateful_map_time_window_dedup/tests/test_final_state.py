import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.json")

def test_pipeline_execution_and_output():
    """Run the pipeline and verify the output contains the correct deduplicated events."""
    # Ensure output.json is removed before running
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    # Run the pipeline
    result = subprocess.run(
        ["python", "run_pipeline.py"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0, f"Pipeline execution failed. Stderr: {result.stderr}\nStdout: {result.stdout}"
    
    assert os.path.exists(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = f.read().strip().split("\n")
        
    # Ignore empty lines
    lines = [line for line in lines if line.strip()]
    
    assert len(lines) == 4, f"Expected exactly 4 output events, but got {len(lines)}."
    
    try:
        events = [json.loads(line) for line in lines]
    except json.JSONDecodeError as e:
        pytest.fail(f"Failed to parse output as JSON lines: {e}")
        
    payloads = [event.get("payload") for event in events]
    
    assert payloads[0] == "A", f"Expected first event payload to be 'A', got {payloads[0]}"
    assert payloads[1] == "B", f"Expected second event payload to be 'B', got {payloads[1]}"
    assert payloads[2] == "A-dup-after-10s", f"Expected third event payload to be 'A-dup-after-10s', got {payloads[2]}"
    assert payloads[3] == "C", f"Expected fourth event payload to be 'C', got {payloads[3]}"
