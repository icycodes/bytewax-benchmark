import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax-task"
INPUT_CSV = os.path.join(PROJECT_DIR, "input.csv")
OUTPUT_CSV = os.path.join(PROJECT_DIR, "output.csv")

@pytest.fixture(scope="module", autouse=True)
def setup_environment():
    # Setup step 1: cd /home/user/bytewax-task (handled by cwd in subprocess)
    
    # Setup step 2: remove output.csv if it exists
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
        
    # Setup step 3: Create input.csv
    input_content = (
        "s1,10.0\n"
        "s2,20.0\n"
        "s1,15.0\n"
        "s1,20.0\n"
        "s2,25.0\n"
        "s1,25.0\n"
        "s3,5.0\n"
    )
    with open(INPUT_CSV, "w") as f:
        f.write(input_content)

def test_pipeline_execution():
    """Verify that the pipeline runs successfully."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow"],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Pipeline execution failed. stderr: {result.stderr}\nstdout: {result.stdout}"

def test_output_csv_contents():
    """Verify that the output CSV contains the correct moving averages."""
    assert os.path.exists(OUTPUT_CSV), "output.csv was not created by the pipeline."
    
    with open(OUTPUT_CSV, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    expected_lines = [
        "s1,10.00",
        "s2,20.00",
        "s1,12.50",
        "s1,15.00",
        "s2,22.50",
        "s1,20.00",
        "s3,5.00"
    ]
    
    assert lines == expected_lines, (
        f"Output CSV contents do not match expected moving averages.\n"
        f"Expected: {expected_lines}\n"
        f"Got: {lines}"
    )
