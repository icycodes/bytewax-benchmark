import os
import subprocess
import json
import math
import pytest

PROJECT_DIR = "/home/user/myproject"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery_dir")

@pytest.fixture(scope="session", autouse=True)
def setup_and_run_pipeline():
    """Set up the environment and run the Bytewax pipeline."""
    # 1. Clean up previous runs
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    if os.path.exists(RECOVERY_DIR):
        import shutil
        shutil.rmtree(RECOVERY_DIR)
        
    os.makedirs(RECOVERY_DIR, exist_ok=True)
    
    # 2. Initialize recovery partitions
    init_res = subprocess.run(
        ["python", "-m", "bytewax.recovery", "recovery_dir", "1"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert init_res.returncode == 0, f"Failed to initialize recovery dir: {init_res.stderr}"
    
    # 3. Create input.jsonl
    input_data = [
        {"sensor_id": "S1", "time": "2026-01-01T00:00:10Z", "temp": 10.0},
        {"sensor_id": "S1", "time": "2026-01-01T00:00:20Z", "temp": 20.0},
        {"sensor_id": "S1", "time": "2026-01-01T00:00:40Z", "temp": 30.0}
    ]
    with open(INPUT_FILE, "w") as f:
        for record in input_data:
            f.write(json.dumps(record) + "\n")
            
    # 4. Run the pipeline
    run_res = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow", "-r", "./recovery_dir", "-s", "1", "-b", "0"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert run_res.returncode == 0, f"Bytewax pipeline failed to run: {run_res.stderr}\nStdout: {run_res.stdout}"

def test_output_file_exists():
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."

def test_first_window_results():
    """Verify the first window (00:00:00 to 00:01:00)."""
    records = []
    with open(OUTPUT_FILE, "r") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
                
    # Find the first window record
    first_window = None
    for r in records:
        if r.get("window_start") == "2026-01-01T00:00:00Z" and r.get("window_end") == "2026-01-01T00:01:00Z":
            first_window = r
            break
            
    assert first_window is not None, "First window (00:00:00 to 00:01:00) results not found in output."
    assert first_window.get("sensor_id") == "S1", f"Expected sensor_id 'S1', got {first_window.get('sensor_id')}"
    assert first_window.get("mean") == 20.0, f"Expected mean 20.0, got {first_window.get('mean')}"
    
    stddev = first_window.get("stddev")
    assert stddev is not None, "stddev field is missing from output."
    assert math.isclose(stddev, 8.16496580927726, rel_tol=1e-3), f"Expected stddev approx 8.165, got {stddev}"

def test_second_window_results():
    """Verify the second window (00:00:30 to 00:01:30)."""
    records = []
    with open(OUTPUT_FILE, "r") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
                
    # Find the second window record
    second_window = None
    for r in records:
        if r.get("window_start") == "2026-01-01T00:00:30Z" and r.get("window_end") == "2026-01-01T00:01:30Z":
            second_window = r
            break
            
    assert second_window is not None, "Second window (00:00:30 to 00:01:30) results not found in output."
    assert second_window.get("sensor_id") == "S1", f"Expected sensor_id 'S1', got {second_window.get('sensor_id')}"
    assert second_window.get("mean") == 30.0, f"Expected mean 30.0, got {second_window.get('mean')}"
    
    stddev = second_window.get("stddev")
    assert stddev is not None, "stddev field is missing from output."
    assert math.isclose(stddev, 0.0, rel_tol=1e-3, abs_tol=1e-3), f"Expected stddev 0.0, got {stddev}"
