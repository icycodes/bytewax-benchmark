import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.json")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery_dir")
OUTPUT_LOG = os.path.join(PROJECT_DIR, "output.log")

@pytest.fixture(scope="module", autouse=True)
def setup_and_run_pipeline():
    # 1. Prepare Input Data
    input_data = [["A", 1], ["B", 1], ["C", 1], ["A", 2], ["D", 4], ["B", 1], ["C", 5]]
    with open(INPUT_FILE, "w") as f:
        json.dump(input_data, f)
    
    # Clean up recovery dir if it exists
    if os.path.exists(RECOVERY_DIR):
        import shutil
        shutil.rmtree(RECOVERY_DIR)
        
    # Clean up output log if it exists
    if os.path.exists(OUTPUT_LOG):
        os.remove(OUTPUT_LOG)

    # 2. Initialize Recovery
    init_result = subprocess.run(
        ["python", "-m", "bytewax.recovery", "./recovery_dir", "1"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert init_result.returncode == 0, f"Failed to initialize recovery: {init_result.stderr}"

    # 3. Run Pipeline
    with open(OUTPUT_LOG, "w") as f:
        run_result = subprocess.run(
            ["python", "-m", "bytewax.run", "pipeline:flow", "-r", "./recovery_dir", "-s", "10", "-b", "0"],
            cwd=PROJECT_DIR,
            stdout=f,
            stderr=subprocess.PIPE,
            text=True
        )
    
    assert run_result.returncode == 0, f"Pipeline execution failed: {run_result.stderr}"

def test_pipeline_output():
    assert os.path.exists(OUTPUT_LOG), f"Output log {OUTPUT_LOG} was not created."
    
    with open(OUTPUT_LOG, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) > 0, "Output log is empty."
    
    last_line = lines[-1]
    try:
        last_output = json.loads(last_line)
    except json.JSONDecodeError:
        pytest.fail(f"Last line of output is not valid JSON: {last_line}")
        
    expected_output = [["C", 6], ["D", 4], ["A", 3]]
    
    assert last_output == expected_output, f"Expected final Top-3 to be {expected_output}, but got {last_output}"
