import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/myproject"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

INPUT_DATA = [
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:00Z"},
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:05Z"},
    {"user_id": "u2", "event_id": "e1", "timestamp": "2023-01-01T12:00:06Z"},
    {"user_id": "u1", "event_id": "e2", "timestamp": "2023-01-01T12:00:07Z"},
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:11Z"},
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:15Z"}
]

EXPECTED_OUTPUT_DATA = [
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:00Z"},
    {"user_id": "u2", "event_id": "e1", "timestamp": "2023-01-01T12:00:06Z"},
    {"user_id": "u1", "event_id": "e2", "timestamp": "2023-01-01T12:00:07Z"},
    {"user_id": "u1", "event_id": "e1", "timestamp": "2023-01-01T12:00:11Z"}
]

@pytest.fixture(scope="session", autouse=True)
def setup_and_run():
    # Setup input.jsonl
    with open(INPUT_FILE, "w") as f:
        for item in INPUT_DATA:
            f.write(json.dumps(item) + "\n")
    
    # Remove output.jsonl if exists
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    # Run the dataflow
    result = subprocess.run(
        ["python3", "run.py"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    
    # We do not assert return code here to let the test fail with a good message if output is missing
    yield result

def test_output_file_exists(setup_and_run):
    result = setup_and_run
    assert result.returncode == 0, f"run.py failed with stderr: {result.stderr}"
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."

def test_output_contents(setup_and_run):
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = f.readlines()
        
    assert len(lines) == 4, f"Expected exactly 4 lines in output.jsonl, got {len(lines)}"
    
    # Parse each line
    actual_data = []
    for line in lines:
        try:
            actual_data.append(json.loads(line.strip()))
        except json.JSONDecodeError:
            pytest.fail(f"Line in output.jsonl is not valid JSON: {line}")
            
    # We check if the set of dictionaries matches exactly
    # Since order might vary if parallel (though for single worker it should be deterministic),
    # we verify that each expected item is in actual_data exactly once.
    # To do this safely, we can sort by timestamp and user_id.
    
    def sort_key(x):
        return (x.get("timestamp", ""), x.get("user_id", ""), x.get("event_id", ""))
        
    sorted_actual = sorted(actual_data, key=sort_key)
    sorted_expected = sorted(EXPECTED_OUTPUT_DATA, key=sort_key)
    
    assert sorted_actual == sorted_expected, \
        f"Output data does not match expected deduplicated events.\nExpected: {sorted_expected}\nActual: {sorted_actual}"
