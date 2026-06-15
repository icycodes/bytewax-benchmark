import os
import subprocess
import glob
import json
import pytest
import re

PROJECT_DIR = "/home/user/bytewax-sink"
OUT_DIR = os.path.join(PROJECT_DIR, "out")
RUN_ID = os.environ.get("ZEALT_RUN_ID", "test-run-123")

@pytest.fixture(scope="session", autouse=True)
def run_dataflow():
    """Setup: clean out directory and run the dataflow script."""
    os.makedirs(OUT_DIR, exist_ok=True)
    # Clean up existing files
    for f in glob.glob(os.path.join(OUT_DIR, "*")):
        os.remove(f)
    
    # Run the dataflow
    env = os.environ.copy()
    env["ZEALT_RUN_ID"] = RUN_ID
    
    result = subprocess.run(
        ["python3", "-m", "bytewax.run", "run:flow", "-w", "4"],
        cwd=PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Dataflow execution failed: {result.stderr}"
    yield

def test_output_directory_files_count():
    """Verify at least 10 files are created in the output directory."""
    files = os.listdir(OUT_DIR)
    assert len(files) >= 10, f"Expected at least 10 files, but found {len(files)}."

def test_output_file_names():
    """Verify all filenames match the expected pattern."""
    files = os.listdir(OUT_DIR)
    pattern = re.compile(rf"^output-{RUN_ID}-worker-[0-3]-part-[0-9]+\.jsonl$")
    for filename in files:
        assert pattern.match(filename), f"Filename {filename} does not match expected pattern."

def test_total_record_count():
    """Verify exactly 200 records were written in total."""
    total_records = 0
    for filepath in glob.glob(os.path.join(OUT_DIR, "*.jsonl")):
        with open(filepath, "r") as f:
            total_records += sum(1 for _ in f)
    assert total_records == 200, f"Expected exactly 200 records, found {total_records}."

def test_file_line_limits():
    """Verify no file has more than 20 lines."""
    for filepath in glob.glob(os.path.join(OUT_DIR, "*.jsonl")):
        with open(filepath, "r") as f:
            line_count = sum(1 for _ in f)
        assert line_count <= 20, f"File {filepath} has {line_count} lines, which exceeds the limit of 20."

def test_json_content_and_values():
    """Verify JSON content, min/max values, and worker distribution."""
    values = []
    workers = set()
    
    for filepath in glob.glob(os.path.join(OUT_DIR, "*.jsonl")):
        with open(filepath, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                values.append(record["value"])
                workers.add(record["worker"])
                
    values.sort()
    assert len(values) > 0, "No records found to check values."
    assert values[0] == 0, f"Expected minimum value 0, found {values[0]}."
    assert values[-1] == 199, f"Expected maximum value 199, found {values[-1]}."
    assert len(workers) >= 2, f"Expected data to be distributed across multiple workers, but only found workers: {workers}."
