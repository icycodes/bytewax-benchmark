import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax-task"
RUN_ID = "zr-test123"
OUTPUT_FILE = f"output-{RUN_ID}.jsonl"
INPUT_FILE = "input.jsonl"

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    # 1. cd /home/user/bytewax-task
    # We will use cwd=PROJECT_DIR in subprocess calls
    
    # 2. export ZEALT_RUN_ID="zr-test123"
    os.environ["ZEALT_RUN_ID"] = RUN_ID
    
    # 3. if [ -f "output-${ZEALT_RUN_ID}.jsonl" ]; then rm "output-${ZEALT_RUN_ID}.jsonl"; fi
    output_path = os.path.join(PROJECT_DIR, OUTPUT_FILE)
    if os.path.exists(output_path):
        os.remove(output_path)
        
    # 4. Create a test `input.jsonl`
    input_content = """{"type": "metric", "device_id": "D1", "payload": {"temperature": 105.0}}
{"type": "metric", "device_id": "D2", "payload": {"temperature": 95.0}}
invalid_json_string_here
{"type": "config", "device_id": "D2", "payload": {"threshold": 90.0}}
{"type": "metric", "device_id": "D2", "payload": {"temperature": 95.0}}
{"type": "config", "device_id": "D1", "payload": {"threshold": 110.0}}
{"type": "metric", "device_id": "D1", "payload": {"temperature": 108.0}}
{"type": "metric", "device_id": "D1", "payload": {"temperature": 115.0}}
{"type": "unknown", "device_id": "D3"}
"""
    input_path = os.path.join(PROJECT_DIR, INPUT_FILE)
    with open(input_path, "w") as f:
        f.write(input_content)

def test_pipeline_execution():
    """Run the pipeline and check for success."""
    env = os.environ.copy()
    env["ZEALT_RUN_ID"] = RUN_ID
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        env=env
    )
    assert result.returncode == 0, f"Pipeline execution failed. stdout: {result.stdout}\nstderr: {result.stderr}"

def test_output_content():
    """Verify the output JSONL file has the expected content."""
    output_path = os.path.join(PROJECT_DIR, OUTPUT_FILE)
    assert os.path.exists(output_path), f"Output file {OUTPUT_FILE} was not created."
    
    with open(output_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 3, f"Expected exactly 3 lines in output, found {len(lines)}."
    
    parsed_objects = []
    for i, line in enumerate(lines):
        try:
            parsed_objects.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"Line {i+1} in output is not valid JSON: {line}")
            
    expected_1 = {"device_id": "D1", "alert_type": "temperature_high", "value": 105.0, "threshold": 100.0}
    expected_2 = {"device_id": "D2", "alert_type": "temperature_high", "value": 95.0, "threshold": 90.0}
    expected_3 = {"device_id": "D1", "alert_type": "temperature_high", "value": 115.0, "threshold": 110.0}
    
    assert expected_1 in parsed_objects, f"Expected object {expected_1} not found in output."
    assert expected_2 in parsed_objects, f"Expected object {expected_2} not found in output."
    assert expected_3 in parsed_objects, f"Expected object {expected_3} not found in output."
