import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/ad_pipeline"
JOINED_FILE = os.path.join(PROJECT_DIR, "joined.jsonl")

def test_pipeline_execution():
    """Run the bytewax pipeline and check for success."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow", "-r", "./recovery_dir"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline execution failed. stdout: {result.stdout}\nstderr: {result.stderr}"

def test_output_file_exists():
    """Verify that joined.jsonl exists."""
    assert os.path.isfile(JOINED_FILE), f"Output file {JOINED_FILE} was not created."

def test_output_file_contents():
    """Verify that the joined output contains the correct records."""
    with open(JOINED_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    
    assert len(lines) == 2, f"Expected exactly 2 lines in {JOINED_FILE}, but got {len(lines)}."
    
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"Line in {JOINED_FILE} is not valid JSON: {line}")
            
    expected_record_1 = {"ad_id": "1", "user_id": "u1", "click_time": "2026-06-15T10:00:00Z"}
    expected_record_3 = {"ad_id": "3", "user_id": "u3", "click_time": "2026-06-15T10:05:00Z"}
    
    assert expected_record_1 in records, f"Expected record {expected_record_1} not found in output."
    assert expected_record_3 in records, f"Expected record {expected_record_3} not found in output."
